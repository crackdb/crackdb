use crate::{
    expressions::Expression, logical_plans::LogicalPlan,
    optimizer::OptimizerContextForExpr, DBError, DBResult,
};

use super::Rule;

pub struct ResolveFunctionsRule {}

impl Rule<LogicalPlan> for ResolveFunctionsRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &<LogicalPlan as crate::optimizer::OptimizerNode>::Context,
    ) -> crate::DBResult<Option<LogicalPlan>> {
        node.transform_exprs(self, context)
    }
}

impl Rule<Expression> for ResolveFunctionsRule {
    fn apply(
        &self,
        node: &Expression,
        context: &<Expression as crate::optimizer::OptimizerNode>::Context,
    ) -> crate::DBResult<Option<Expression>> {
        node.transform_bottom_up(context, Self::resolve_functions)
    }
}

impl ResolveFunctionsRule {
    fn resolve_functions(
        expr: &Expression,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Option<Expression>> {
        match expr {
            Expression::UnResolvedFunction { name, args } => {
                let func = context
                    .functions_registry
                    .get_function(name.as_str(), args)?
                    .ok_or(DBError::Unknown(format!("Unrecognized function {name}")))?;
                Ok(Some(Expression::Function(func)))
            }
            _ => Ok(None),
        }
    }
}
