use crate::{
    data_types::DataType,
    expressions::{BooleanExpr, Expression},
    logical_plans::LogicalPlan,
    optimizer::{OptimizerContext, OptimizerContextForExpr},
    DBResult,
};

use super::Rule;

pub struct ResolveLiteralTypesRule {}

impl Rule<LogicalPlan> for ResolveLiteralTypesRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &OptimizerContext,
    ) -> crate::DBResult<Option<LogicalPlan>> {
        node.transform_exprs(self, context)
    }
}

impl Rule<Expression> for ResolveLiteralTypesRule {
    fn apply(
        &self,
        node: &Expression,
        context: &OptimizerContextForExpr,
    ) -> crate::DBResult<Option<Expression>> {
        match node {
            Expression::BooleanExpr(boolean_expr) => self
                .resolve_literal_types(boolean_expr, context)
                .map(|opt_expr| {
                    opt_expr.map(|expr| Expression::BooleanExpr(Box::new(expr)))
                }),
            _ => Ok(None),
        }
    }
}

impl ResolveLiteralTypesRule {
    fn resolve_literal_types(
        &self,
        expr: &BooleanExpr,
        _context: &OptimizerContextForExpr,
    ) -> DBResult<Option<BooleanExpr>> {
        match expr {
            BooleanExpr::GT { left, right } => {
                self.align_data_type(left, right, |left, right| BooleanExpr::GT {
                    left,
                    right,
                })
            }
            BooleanExpr::GTE { left, right } => {
                self.align_data_type(left, right, |left, right| BooleanExpr::GTE {
                    left,
                    right,
                })
            }
            BooleanExpr::EQ { left, right } => {
                self.align_data_type(left, right, |left, right| BooleanExpr::EQ {
                    left,
                    right,
                })
            }
            BooleanExpr::LT { left, right } => {
                self.align_data_type(left, right, |left, right| BooleanExpr::LT {
                    left,
                    right,
                })
            }
            BooleanExpr::LTE { left, right } => {
                self.align_data_type(left, right, |left, right| BooleanExpr::LTE {
                    left,
                    right,
                })
            }
        }
    }

    fn align_data_type<F>(
        &self,
        left: &Expression,
        right: &Expression,
        builder: F,
    ) -> DBResult<Option<BooleanExpr>>
    where
        F: FnOnce(Expression, Expression) -> BooleanExpr,
    {
        if left.data_type() != DataType::Unknown && right.data_type() == DataType::Unknown
        {
            self.align_data_type_left_to_right(left, right, builder)
        } else if right.data_type() != DataType::Unknown
            && left.data_type() == DataType::Unknown
        {
            self.align_data_type_left_to_right(right, left, |right, left| {
                builder(left, right)
            })
        } else {
            Ok(None)
        }
    }

    /// transform the right expression to align it with left expression data type
    fn align_data_type_left_to_right<F>(
        &self,
        left: &Expression,
        right: &Expression,
        builder: F,
    ) -> DBResult<Option<BooleanExpr>>
    where
        F: FnOnce(Expression, Expression) -> BooleanExpr,
    {
        if let Some(resolved_right) =
            self.transform_expression_with_type_hint(right, left.data_type())?
        {
            if left.data_type() == resolved_right.data_type() {
                Ok(Some(builder(left.clone(), resolved_right)))
            } else {
                // TODO: try best to align the data type
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// transform a expression (focus on literal expressions) with given data type hint.
    /// For example, transform an UnresolvedString into a String or DataTime etc.
    fn transform_expression_with_type_hint(
        &self,
        expr: &Expression,
        type_hint: DataType,
    ) -> DBResult<Option<Expression>> {
        match expr {
            Expression::Literal(literal) => literal
                .cast_or_maintain_precision(type_hint)
                .map(|opt_literal| opt_literal.map(Expression::Literal)),
            // TODO: do we want validate the expression data type with type_hint?
            _ => Ok(None),
        }
    }
}
