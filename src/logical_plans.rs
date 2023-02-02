use crate::expressions::Expression;
use crate::optimizer::rules::Rule;
use crate::optimizer::{OptimizerContext, OptimizerContextForExpr, OptimizerNode};
use crate::tables::RelationSchema;
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
            LogicalPlan::Filter { expression, child } => {
                let opt_new_child = child.transform_bottom_up(context, func)?;
                match opt_new_child {
                    Some(new_child) => {
                        let new_self = LogicalPlan::Filter {
                            expression: expression.clone(),
                            child: Box::new(new_child),
                        };
                        match func(&new_self, context)? {
                            Some(new_self) => Ok(Some(new_self)),
                            None => Ok(Some(new_self)),
                        }
                    }
                    None => func(self, context),
                }
            }
        }
    }

    pub fn transform_exprs(
        &self,
        rule: &dyn Rule<Expression>,
        _context: &OptimizerContext,
    ) -> DBResult<Option<Self>> {
        match self {
            LogicalPlan::UnResolvedScan { .. } => Ok(None),
            LogicalPlan::Scan { .. } => Ok(None),
            LogicalPlan::Filter { expression, child } => {
                let new_child = child.transform_exprs(rule, _context)?;
                let schema = match &new_child {
                    Some(new_child) => new_child.schema()?,
                    None => child.schema()?,
                };
                let context_for_expr = OptimizerContextForExpr::new(schema);
                let new_expr = rule.apply(expression, &context_for_expr)?;
                match (new_child, new_expr) {
                    (None, None) => Ok(None),
                    // TODO: can/should we avoid clone child?
                    (None, Some(expression)) => Ok(Some(LogicalPlan::Filter {
                        expression,
                        child: child.clone(),
                    })),
                    (Some(child), None) => Ok(Some(LogicalPlan::Filter {
                        expression: expression.clone(),
                        child: Box::new(child),
                    })),
                    (Some(child), Some(expression)) => Ok(Some(LogicalPlan::Filter {
                        expression,
                        child: Box::new(child),
                    })),
                }
            }
        }
    }
}
