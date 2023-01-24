use crate::{
    expressions::{BooleanExpr, Expression, Literal},
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
    fn resolve_boolean_expression(
        &self,
        expr: &BooleanExpr,
        context: &OptimizerContextForExpr,
    ) -> DBResult<Option<BooleanExpr>> {
        match expr {
            BooleanExpr::GT { left, right } => {
                self.resolve_boolean_expr_helper(context, left, right, |left, right| {
                    BooleanExpr::GT { left, right }
                })
            }
            BooleanExpr::GTE { left, right } => {
                self.resolve_boolean_expr_helper(context, left, right, |left, right| {
                    BooleanExpr::GTE { left, right }
                })
            }
            BooleanExpr::EQ { left, right } => {
                self.resolve_boolean_expr_helper(context, left, right, |left, right| {
                    BooleanExpr::EQ { left, right }
                })
            }
            BooleanExpr::LT { left, right } => {
                self.resolve_boolean_expr_helper(context, left, right, |left, right| {
                    BooleanExpr::LT { left, right }
                })
            }
            BooleanExpr::LTE { left, right } => {
                self.resolve_boolean_expr_helper(context, left, right, |left, right| {
                    BooleanExpr::LTE { left, right }
                })
            }
        }
    }

    fn resolve_boolean_expr_helper(
        &self,
        context: &OptimizerContextForExpr,
        left: &Expression,
        right: &Expression,
        builder: fn(Expression, Expression) -> BooleanExpr,
    ) -> DBResult<Option<BooleanExpr>> {
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
            Expression::Literal(Literal::UnResolvedNumber(s)) => Ok(Some(
                s.parse::<i32>()
                    .map(|v| Expression::Literal(Literal::Int(v)))?,
            )),
            Expression::Literal(Literal::UnResolvedString(s)) => {
                Ok(Some(Expression::Literal(Literal::String(s.to_string()))))
            }
            Expression::Literal(_) => Ok(None),
            Expression::UnResolvedFieldRef(name) => {
                match context
                    .schema()
                    .get_fields()
                    .iter()
                    .position(|f| f.name().eq(name))
                {
                    Some(idx) => Ok(Some(Expression::FieldRef(
                        idx,
                        context
                            .schema()
                            .get_fields()
                            .get(idx)
                            .unwrap()
                            .data_type()
                            .clone(),
                    ))),
                    None => Err(DBError::Unknown(format!("Cannot find field {}", name))),
                }
            }
            Expression::FieldRef(_, _) => Ok(None),
            Expression::BooleanExpr(boolean_expr) => self
                .resolve_boolean_expression(boolean_expr, context)
                .map(|opt_resolved_expr| {
                    opt_resolved_expr.map(|resolved_expr| {
                        Expression::BooleanExpr(Box::new(resolved_expr))
                    })
                }),
        }
    }
}
