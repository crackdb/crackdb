use crate::aggregators::aggregator_schema;
use crate::expressions::Expression;
use crate::optimizer::rules::Rule;
use crate::optimizer::{OptimizerContext, OptimizerContextForExpr, OptimizerNode};
use crate::tables::{FieldInfo, RelationSchema};
use crate::{DBError, DBResult};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    UnResolvedScan {
        table: String,
    },
    Scan {
        table: String,
        schema: RelationSchema,
    },
    Filter {
        expression: Expression,
        child: Box<LogicalPlan>,
    },
    Projection {
        expressions: Vec<Expression>,
        child: Box<LogicalPlan>,
    },
    Aggregator {
        aggregators: Vec<Expression>,
        groupings: Vec<Expression>,
        child: Box<LogicalPlan>,
    },
    Sort {
        options: Vec<SortOption>,
        child: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub struct SortOption {
    expr: Expression,
    asc: bool,
}

impl SortOption {
    pub fn new(expr: Expression, asc: bool) -> Self {
        Self { expr, asc }
    }

    pub fn asc(&self) -> bool {
        self.asc
    }

    pub fn expr(&self) -> &Expression {
        &self.expr
    }
}

impl OptimizerNode for LogicalPlan {
    type Context = OptimizerContext;
}

impl LogicalPlan {
    pub fn schema(&self) -> DBResult<RelationSchema> {
        match self {
            LogicalPlan::UnResolvedScan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
            LogicalPlan::Scan { table: _, schema } => Ok(schema.clone()),
            LogicalPlan::Filter {
                expression: _,
                child,
            } => child.schema(),
            LogicalPlan::Projection {
                expressions,
                child: _,
            } => {
                let fields = expressions
                    .iter()
                    .map(|expr| FieldInfo::new(expr.to_string(), expr.data_type()))
                    .collect();
                let schema = RelationSchema::new(fields);
                Ok(schema)
            }
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                ..
            } => Ok(aggregator_schema(groupings, aggregators)),
            LogicalPlan::Sort { child, .. } => child.schema(),
        }
    }

    /// transform plan node in bottom-up style.
    pub fn transform_bottom_up(
        &self,
        context: &OptimizerContext,
        func: fn(&Self, &OptimizerContext) -> DBResult<Option<Self>>,
    ) -> DBResult<Option<Self>> {
        match self {
            LogicalPlan::UnResolvedScan { .. } => func(self, context),
            LogicalPlan::Scan { .. } => func(self, context),
            LogicalPlan::Filter { expression, child } => self
                .transform_bottom_up_for_single_child_plan(
                    child,
                    context,
                    func,
                    |updated_child| LogicalPlan::Filter {
                        expression: expression.clone(),
                        child: Box::new(updated_child),
                    },
                ),
            LogicalPlan::Projection { expressions, child } => self
                .transform_bottom_up_for_single_child_plan(
                    child,
                    context,
                    func,
                    |updated_child| LogicalPlan::Projection {
                        expressions: expressions.clone(),
                        child: Box::new(updated_child),
                    },
                ),
            LogicalPlan::Aggregator {
                child,
                aggregators,
                groupings,
            } => self.transform_bottom_up_for_single_child_plan(
                child,
                context,
                func,
                |updated_child| LogicalPlan::Aggregator {
                    aggregators: aggregators.clone(),
                    groupings: groupings.clone(),
                    child: Box::new(updated_child),
                },
            ),
            LogicalPlan::Sort { options, child } => self
                .transform_bottom_up_for_single_child_plan(
                    child,
                    context,
                    func,
                    |updated_child| LogicalPlan::Sort {
                        options: options.clone(),
                        child: Box::new(updated_child),
                    },
                ),
        }
    }

    fn transform_bottom_up_for_single_child_plan(
        &self,
        child: &LogicalPlan,
        context: &OptimizerContext,
        func: fn(&Self, &OptimizerContext) -> DBResult<Option<Self>>,
        builder: impl FnOnce(LogicalPlan) -> LogicalPlan,
    ) -> DBResult<Option<Self>> {
        let opt_new_child = child.transform_bottom_up(context, func)?;
        match opt_new_child {
            Some(new_child) => {
                let new_self = builder(new_child);
                match func(&new_self, context)? {
                    Some(new_self) => Ok(Some(new_self)),
                    None => Ok(Some(new_self)),
                }
            }
            None => func(self, context),
        }
    }

    pub fn transform_exprs(
        &self,
        rule: &dyn Rule<Expression>,
        context: &OptimizerContext,
    ) -> DBResult<Option<Self>> {
        match self {
            LogicalPlan::UnResolvedScan { .. } => Ok(None),
            LogicalPlan::Scan { .. } => Ok(None),
            LogicalPlan::Filter { expression, child } => {
                let expressions = vec![expression.clone()];
                self.transform_exprs_for_single_child_plan(
                    &expressions,
                    child,
                    rule,
                    context,
                    |expressions, child| LogicalPlan::Filter {
                        expression: expressions.into_iter().next().unwrap(),
                        child: Box::new(child),
                    },
                )
            }
            LogicalPlan::Projection { expressions, child } => self
                .transform_exprs_for_single_child_plan(
                    expressions,
                    child,
                    rule,
                    context,
                    |expressions, child| LogicalPlan::Projection {
                        expressions,
                        child: Box::new(child),
                    },
                ),
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                child,
            } => {
                let expressions = aggregators
                    .iter()
                    .chain(groupings.iter())
                    .cloned()
                    .collect();
                self.transform_exprs_for_single_child_plan(
                    &expressions,
                    child,
                    rule,
                    context,
                    |mut expressions, child| {
                        let new_groupings = expressions.split_off(aggregators.len());
                        LogicalPlan::Aggregator {
                            aggregators: expressions,
                            groupings: new_groupings,
                            child: Box::new(child),
                        }
                    },
                )
            }
            LogicalPlan::Sort { options, child } => {
                let expressions = options.iter().map(|opt| opt.expr.clone()).collect();
                self.transform_exprs_for_single_child_plan(
                    &expressions,
                    child,
                    rule,
                    context,
                    |expressions, child| {
                        let new_options = options
                            .iter()
                            .zip(expressions.into_iter())
                            .map(|(opt, new_expr)| SortOption::new(new_expr, opt.asc()))
                            .collect();
                        LogicalPlan::Sort {
                            options: new_options,
                            child: Box::new(child),
                        }
                    },
                )
            }
        }
    }

    fn transform_exprs_for_single_child_plan(
        &self,
        expressions: &Vec<Expression>,
        child: &LogicalPlan,
        rule: &dyn Rule<Expression>,
        context: &OptimizerContext,
        builder: impl FnOnce(Vec<Expression>, LogicalPlan) -> LogicalPlan,
    ) -> DBResult<Option<Self>> {
        let new_child = child.transform_exprs(rule, context)?;
        let schema = match &new_child {
            Some(new_child) => new_child.schema()?,
            None => child.schema()?,
        };
        let context_for_expr = OptimizerContextForExpr::new(schema);
        let mut any_expr_transformed = false;
        let mut new_exprs = Vec::new();
        for expr in expressions {
            if let Some(transformed) = rule.apply(expr, &context_for_expr)? {
                any_expr_transformed = true;
                new_exprs.push(transformed);
            } else {
                new_exprs.push(expr.clone());
            }
        }
        match (new_child, any_expr_transformed) {
            (None, false) => Ok(None),
            // TODO: can/should we avoid clone child?
            (None, true) => Ok(Some(builder(new_exprs, child.clone()))),
            (Some(child), false) => Ok(Some(builder(new_exprs, child))),
            (Some(child), true) => Ok(Some(builder(new_exprs, child))),
        }
    }
}
