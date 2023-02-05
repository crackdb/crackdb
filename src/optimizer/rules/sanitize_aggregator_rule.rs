use std::collections::HashMap;

use crate::{
    expressions::Expression,
    logical_plans::LogicalPlan,
    optimizer::{OptimizerContext, OptimizerContextForExpr, OptimizerNode},
    tables::RelationSchema,
    DBResult,
};

use super::Rule;

/// This rule will extract any aggregator exprs from Projection node and move them to Aggregator node.
///
/// Examples:
/// ```ignore
/// Projection { projection: [(sum(x) + sum(y)) as total, orderId] }
///     -- Aggregator { aggregators: [], groupings: [orderId] }
/// ```
/// will be transformed to:
/// ```ignore
/// Projection { projections: [("sum(x)" + "sum(y)") as total, orderId] }
///     -- Aggregator { aggregators: [sum(x), sum(y)], groupings: [orderId] }
/// ```
///
pub struct SanitizeAggregatorRule {}

impl Rule<LogicalPlan> for SanitizeAggregatorRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &<LogicalPlan as OptimizerNode>::Context,
    ) -> DBResult<Option<LogicalPlan>> {
        node.transform_bottom_up(context, Self::prune_groupings_from_aggregators)
    }
}

impl SanitizeAggregatorRule {
    fn prune_groupings_from_aggregators(
        node: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        match node {
            LogicalPlan::Projection { expressions, child } if is_aggregator(child) => {
                // find all aggregators from projection expressions
                let mut aggregators_map = HashMap::new();
                let mut updated_projections = vec![];
                let context = OptimizerContextForExpr::new(RelationSchema::empty());
                for expr in expressions {
                    // update projection expr with refs to aggregator exprs, e.g.:
                    // `sum(x) + sum(y)` to `"sum(x)" + "sum(y)"
                    let opt_new =
                        expr.transform_bottom_up(&context, &mut |e, context| {
                            if context.functions_registry().is_aggregator(e) {
                                aggregators_map.insert(e.to_string(), e.clone());
                                Ok(Some(Expression::UnResolvedFieldRef(e.to_string())))
                            } else {
                                Ok(None)
                            }
                        })?;

                    match opt_new {
                        Some(updated) => updated_projections.push(updated),
                        None => updated_projections.push(expr.clone()),
                    };
                }

                // update child Aggregator with new extracted aggregators
                if aggregators_map.is_empty() {
                    Ok(None)
                } else {
                    let updated_child = match child.as_ref() {
                        LogicalPlan::Aggregator {
                            aggregators,
                            groupings,
                            child,
                        } => {
                            aggregators.iter().for_each(|a| {
                                aggregators_map.remove(a.to_string().as_str());
                            });
                            // TODO: clone existing aggregator directly if aggregators_map is empty
                            let mut all_aggregators = aggregators.clone();
                            all_aggregators.extend(aggregators_map.into_values());
                            LogicalPlan::Aggregator {
                                aggregators: all_aggregators,
                                groupings: groupings.clone(),
                                child: child.clone(),
                            }
                        }
                        _ => panic!("should never happen"),
                    };
                    let updated_projection = LogicalPlan::Projection {
                        expressions: updated_projections,
                        child: Box::new(updated_child),
                    };
                    Ok(Some(updated_projection))
                }
            }
            _ => Ok(None),
        }
    }
}

fn is_aggregator(child: &LogicalPlan) -> bool {
    matches!(child, LogicalPlan::Aggregator { .. })
}
