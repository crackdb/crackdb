use std::collections::HashMap;

use crate::{
    expressions::Expression,
    logical_plans::{LogicalPlan, SortOption},
    optimizer::{OptimizerContext, OptimizerContextForExpr},
    tables::RelationSchema,
    DBResult,
};

use super::Rule;

/// This rule can help resolve aggregators in Having, Sort, Projection clause by pushing them down into
/// Aggregator node.
pub struct PushDownAggregatorsRule {}

impl Rule<LogicalPlan> for PushDownAggregatorsRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &<LogicalPlan as crate::optimizer::OptimizerNode>::Context,
    ) -> crate::DBResult<Option<LogicalPlan>> {
        node.transform_bottom_up(context, Self::resolve_or_push_down_aggregators)
    }
}

impl PushDownAggregatorsRule {
    fn resolve_or_push_down_aggregators(
        node: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        let context = OptimizerContextForExpr::new(RelationSchema::empty());
        match node {
            LogicalPlan::Projection { expressions, child } => {
                let expressions = expressions.iter().collect::<Vec<_>>();
                Self::resolve_or_push_down_aggregators_helpers(
                    &context,
                    &expressions,
                    child,
                    |expressions, updated_child| LogicalPlan::Projection {
                        expressions,
                        child: Box::new(updated_child),
                    },
                )
            }
            LogicalPlan::Sort { options, child } => {
                let expressions = options
                    .iter()
                    .map(|option| option.expr())
                    .collect::<Vec<_>>();
                Self::resolve_or_push_down_aggregators_helpers(
                    &context,
                    &expressions,
                    child,
                    |expressions, updated_child| {
                        let new_options =
                            Iterator::zip(options.iter(), expressions.into_iter())
                                .map(|(option, expr)| SortOption::new(expr, option.asc()))
                                .collect();
                        LogicalPlan::Sort {
                            options: new_options,
                            child: Box::new(updated_child),
                        }
                    },
                )
            }
            LogicalPlan::UnResolvedHaving { prediction, child } => {
                let expressions = vec![prediction];
                Self::resolve_or_push_down_aggregators_helpers(
                    &context,
                    &expressions,
                    child,
                    |expressions, updated_child| LogicalPlan::Filter {
                        expression: expressions.into_iter().next().unwrap(),
                        child: Box::new(updated_child),
                    },
                )
            }
            _ => Ok(None),
        }
    }

    /// extract aggregators from expression and put them into the given aggregators Map.
    fn extract_aggregators(
        context: &OptimizerContextForExpr,
        expression: &Expression,
        aggregators: &mut HashMap<String, Expression>,
    ) -> DBResult<()> {
        expression
            .transform_top_down(context, &mut |node, context| {
                if context.functions_registry.is_aggregator(node) {
                    aggregators.insert(node.sematic_id(), node.clone());
                }
                Ok(None)
            })
            .map(|_| ())
    }

    fn resolve_or_push_down_aggregators_helpers(
        context: &OptimizerContextForExpr,
        expressions: &[&Expression],
        child: &LogicalPlan,
        builder: impl Fn(Vec<Expression>, LogicalPlan) -> LogicalPlan,
    ) -> DBResult<Option<LogicalPlan>> {
        let mut aggregators = HashMap::new();
        for expr in expressions {
            Self::extract_aggregators(context, expr, &mut aggregators)?;
        }

        // stop if no aggregators found
        if aggregators.is_empty() {
            return Ok(None);
        }

        let aggs_to_resolve = aggregators.values().collect::<Vec<_>>();
        match Self::resolve_or_push_down(context, child, &aggs_to_resolve)? {
            (Some(updated_child), resolved_map)
                if resolved_map.len() == aggs_to_resolve.len() =>
            {
                let new_expressions = Self::transform_exprs_with_resolved_agg(
                    expressions,
                    &resolved_map,
                    context,
                )?;

                let updated_schema = updated_child.schema()?;
                let updated_node = builder(new_expressions, updated_child);
                if !Self::has_projection(&updated_node)
                    && child.schema()? != updated_schema
                {
                    // create a new projection to maintain the output schema after new aggregators
                    // being push down
                    let expressions = child
                        .schema()?
                        .get_fields()
                        .iter()
                        .enumerate()
                        .map(|(index, f)| Expression::FieldRef {
                            name: f.name().to_owned(),
                            index,
                            data_type: f.data_type().clone(),
                        })
                        .collect();
                    let projection = LogicalPlan::Projection {
                        expressions,
                        child: Box::new(updated_node),
                    };
                    Ok(Some(projection))
                } else {
                    Ok(Some(updated_node))
                }
            }
            (None, resolved_map) if resolved_map.len() == aggs_to_resolve.len() => {
                let new_expressions = Self::transform_exprs_with_resolved_agg(
                    expressions,
                    &resolved_map,
                    context,
                )?;
                Ok(Some(builder(new_expressions, child.clone())))
            }
            _ => Ok(None),
        }
    }

    /// transform the given expressions by replacing unresolved aggregators with resolved ones
    fn transform_exprs_with_resolved_agg(
        expressions: &[&Expression],
        resolved_map: &HashMap<String, Expression>,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Vec<Expression>> {
        expressions
            .iter()
            .map(|expr| {
                Self::transform_with_resolved_agg(expr, resolved_map, context).map(|e| {
                    match e {
                        Some(new_expr) => new_expr,
                        None => (*expr).clone(),
                    }
                })
            })
            .collect::<DBResult<Vec<_>>>()
    }

    /// transform the given expr by replacing unresolved aggregators with resolved ones
    fn transform_with_resolved_agg(
        expr: &Expression,
        resolved_map: &HashMap<String, Expression>,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Option<Expression>> {
        expr.transform_top_down(context, &mut |node, context| {
            let semantic_id = node.sematic_id();
            if context.functions_registry.is_aggregator(node) {
                if let Some(resolved) = resolved_map.get(&semantic_id) {
                    return Ok(Some(resolved.clone()));
                }
            }
            Ok(None)
        })
    }

    /// resolve exprs agaist the given node, or push the expr into the given node if resolve
    /// failed.
    fn resolve_or_push_down(
        _context: &OptimizerContextForExpr,
        node: &LogicalPlan,
        exprs: &[&Expression],
    ) -> DBResult<(Option<LogicalPlan>, HashMap<String, Expression>)> {
        match node {
            LogicalPlan::UnResolvedScan { .. } => Ok((None, HashMap::new())),
            LogicalPlan::Scan { .. } => Ok((None, HashMap::new())),
            LogicalPlan::Projection { expressions, child } => {
                let (resolved_exprs, push_down_exprs) =
                    Self::resolve_exprs_against_node_outputs(exprs, node);
                let resolved_map = Self::build_resolved_map(exprs, resolved_exprs);

                if push_down_exprs.is_empty() {
                    // everything is resolved, push down will not happen
                    Ok((None, resolved_map))
                } else {
                    let mut new_expressions = expressions.to_vec();
                    new_expressions.extend(push_down_exprs.into_iter().cloned());
                    let new_plan = LogicalPlan::Projection {
                        expressions: new_expressions,
                        child: child.clone(),
                    };
                    Ok((Some(new_plan), resolved_map))
                }
            }
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                child,
            } => {
                let (resolved_exprs, push_down_exprs) =
                    Self::resolve_exprs_against_node_outputs(exprs, node);
                let resolved_map = Self::build_resolved_map(exprs, resolved_exprs);

                // push down: if any unresolved exprs, append them to the end of aggregators
                if push_down_exprs.is_empty() {
                    Ok((None, resolved_map))
                } else {
                    let mut new_aggregators = aggregators.clone();
                    new_aggregators.extend(push_down_exprs.into_iter().cloned());
                    let updated_plan = LogicalPlan::Aggregator {
                        aggregators: new_aggregators,
                        groupings: groupings.clone(),
                        child: child.clone(),
                    };
                    Ok((Some(updated_plan), resolved_map))
                }
            }
            LogicalPlan::Sort { .. } => Ok((None, HashMap::new())),
            LogicalPlan::Limit { .. } => Ok((None, HashMap::new())),
            LogicalPlan::Filter { expression, child } => {
                // ask child to do the resolve or push down, since current not doesn't do
                // projection
                match Self::resolve_or_push_down(_context, child, exprs)? {
                    (Some(updated_child), resolved) => Ok((
                        Some(LogicalPlan::Filter {
                            expression: expression.clone(),
                            child: Box::new(updated_child),
                        }),
                        resolved,
                    )),
                    (None, resolved) => Ok((None, resolved)),
                }
            }
            LogicalPlan::UnResolvedHaving { prediction, child } => {
                // ask child to do the resolve or push down, since current not doesn't do
                // projection
                match Self::resolve_or_push_down(_context, child, exprs)? {
                    (Some(updated_child), resolved) => Ok((
                        Some(LogicalPlan::UnResolvedHaving {
                            prediction: prediction.clone(),
                            child: Box::new(updated_child),
                        }),
                        resolved,
                    )),
                    (None, resolved) => Ok((None, resolved)),
                }
            }
        }
    }

    fn build_resolved_map(
        unresolved_exprs: &[&Expression],
        resolved_exprs: Vec<Expression>,
    ) -> HashMap<String, Expression> {
        let mut resolved_map = HashMap::new();
        for (unresolved, resolved) in
            Iterator::zip(unresolved_exprs.iter(), resolved_exprs.into_iter())
        {
            resolved_map.insert(unresolved.sematic_id(), resolved);
        }
        resolved_map
    }

    /// semantically resolve the given exprs against the outputs of the given node
    ///
    /// for convinience, this function returns a list of resolved expressions and a list of
    /// expressions for push down.
    fn resolve_exprs_against_node_outputs<'a>(
        exprs: &[&'a Expression],
        node: &LogicalPlan,
    ) -> (Vec<Expression>, Vec<&'a Expression>) {
        let outputs = Self::plan_outputs(node);
        let num_outpus = outputs.len();
        let mut semantic_map = HashMap::new();
        for (idx, output) in outputs.into_iter().enumerate() {
            semantic_map.insert(output.sematic_id(), (idx, output));
        }

        // resolve exprs against child outputs
        let mut resolved_exprs = Vec::with_capacity(exprs.len());
        let mut push_down_exprs = Vec::new();
        for expr in exprs {
            match semantic_map.get(&expr.sematic_id()) {
                Some((index, output)) => {
                    let resolved_by_outpus = Expression::FieldRef {
                        name: output.to_string(),
                        index: *index,
                        data_type: output.data_type(),
                    };
                    resolved_exprs.push(resolved_by_outpus);
                }
                None => {
                    // add unresolved expressions to push down list
                    // assume the push down works by appending the expr to the end of node's outputs
                    let resolved_by_push_down = Expression::FieldRef {
                        name: expr.to_string(),
                        index: num_outpus + push_down_exprs.len(),
                        data_type: expr.data_type(),
                    };
                    resolved_exprs.push(resolved_by_push_down);
                    push_down_exprs.push(*expr);
                }
            }
        }

        (resolved_exprs, push_down_exprs)
    }

    /// the semantic outputs of a LogicalPlan node
    fn plan_outputs(node: &LogicalPlan) -> Vec<&Expression> {
        match node {
            LogicalPlan::UnResolvedScan { .. } => vec![],
            LogicalPlan::Scan { .. } => vec![],
            LogicalPlan::Filter { child, .. } => Self::plan_outputs(child),
            LogicalPlan::Projection { expressions, .. } => expressions.iter().collect(),
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                ..
            } => Iterator::chain(groupings.iter(), aggregators.iter()).collect(),
            LogicalPlan::Sort { child, .. } => Self::plan_outputs(child),
            LogicalPlan::Limit { child, .. } => Self::plan_outputs(child),
            LogicalPlan::UnResolvedHaving { child, .. } => Self::plan_outputs(child),
        }
    }

    /// whether or not the node has re-projection of the logical plan
    fn has_projection(node: &LogicalPlan) -> bool {
        matches!(
            node,
            LogicalPlan::UnResolvedScan { .. }
                | LogicalPlan::Scan { .. }
                | LogicalPlan::Projection { .. }
                | LogicalPlan::Aggregator { .. }
        )
    }
}
