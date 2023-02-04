use crate::{
    expressions::Expression,
    logical_plans::LogicalPlan,
    optimizer::{OptimizerContext, OptimizerContextForExpr},
    DBError, DBResult,
};

use super::Rule;

pub struct ResolveExprRule {}

// TODO: how can we avoid this verbose impl?
impl Rule<LogicalPlan> for ResolveExprRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        node.transform_exprs(self, context)
    }
}

impl Rule<Expression> for ResolveExprRule {
    fn apply(
        &self,
        node: &Expression,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Option<Expression>> {
        node.transform_bottom_up(context, Self::resolve_expression)
    }
}

impl ResolveExprRule {
    pub(crate) fn resolve_expression(
        node: &Expression,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Option<Expression>> {
        match node {
            Expression::Literal(_) => Ok(None),
            Expression::UnResolvedFieldRef(name) => {
                match context
                    .schema()
                    .get_fields()
                    .iter()
                    .position(|f| f.name().eq(name))
                {
                    Some(idx) => Ok(Some(Expression::FieldRef {
                        name: name.to_string(),
                        index: idx,
                        data_type: context
                            .schema()
                            .get_fields()
                            .get(idx)
                            .unwrap()
                            .data_type()
                            .clone(),
                    })),
                    None => Err(DBError::Unknown(format!("Cannot find field {name}"))),
                }
            }
            Expression::FieldRef { .. } => Ok(None),
            Expression::BinaryOp { .. } => Ok(None),
            Expression::UnaryOp { .. } => Ok(None),
            Expression::Alias { .. } => Ok(None),
            Expression::Function(_) => Ok(None),
            Expression::UnResolvedFunction { .. } => Ok(None),
        }
    }
}
