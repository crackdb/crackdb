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
        self.resolve_expression(node, context)
    }
}

impl ResolveExprRule {
    fn resolve_binary_expr_helper<F>(
        &self,
        context: &OptimizerContextForExpr,
        left: &Expression,
        right: &Expression,
        builder: F,
    ) -> DBResult<Option<Expression>>
    where
        F: FnOnce(Expression, Expression) -> Expression,
    {
        match (
            self.resolve_expression(left, context)?,
            self.resolve_expression(right, context)?,
        ) {
            (None, None) => Ok(None),
            (None, Some(right)) => Ok(Some(builder(left.clone(), right))),
            (Some(left), None) => Ok(Some(builder(left, right.clone()))),
            (Some(left), Some(right)) => Ok(Some(builder(left, right))),
        }
    }

    fn resolve_expression(
        &self,
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
            Expression::BinaryOp { op, left, right } => {
                self.resolve_binary_expr_helper(context, left, right, |left, right| {
                    Expression::BinaryOp {
                        op: op.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                })
            }
            Expression::UnaryOp { op, input } => {
                match self.resolve_expression(input, context)? {
                    Some(input) => Ok(Some(Expression::UnaryOp {
                        op: op.clone(),
                        input: Box::new(input),
                    })),
                    None => Ok(None),
                }
            }
            Expression::Alias { alias, child } => {
                match self.resolve_expression(child, context)? {
                    Some(input) => Ok(Some(Expression::Alias {
                        alias: alias.to_string(),
                        child: Box::new(input),
                    })),
                    None => Ok(None),
                }
            }
        }
    }
}
