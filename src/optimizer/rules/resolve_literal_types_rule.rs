use crate::{
    data_types::DataType,
    expressions::Expression,
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
        node.transform_bottom_up(context, Self::resolve_literal_type)
    }
}

impl ResolveLiteralTypesRule {
    fn resolve_literal_type(
        expr: &Expression,
        _context: &OptimizerContextForExpr,
    ) -> DBResult<Option<Expression>> {
        match expr {
            Expression::BinaryOp { op, left, right } => {
                Self::align_data_type(left, right, |left, right| Expression::BinaryOp {
                    op: op.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            _ => Ok(None),
        }
    }
    fn align_data_type<F>(
        left: &Expression,
        right: &Expression,
        builder: F,
    ) -> DBResult<Option<Expression>>
    where
        F: FnOnce(Expression, Expression) -> Expression,
    {
        if left.data_type() != DataType::Unknown && right.data_type() == DataType::Unknown
        {
            Self::align_data_type_left_to_right(left, right, builder)
        } else if right.data_type() != DataType::Unknown
            && left.data_type() == DataType::Unknown
        {
            Self::align_data_type_left_to_right(right, left, |right, left| {
                builder(left, right)
            })
        } else {
            Ok(None)
        }
    }

    /// transform the right expression to align it with left expression data type
    fn align_data_type_left_to_right<F>(
        left: &Expression,
        right: &Expression,
        builder: F,
    ) -> DBResult<Option<Expression>>
    where
        F: FnOnce(Expression, Expression) -> Expression,
    {
        if let Some(resolved_right) =
            Self::transform_expression_with_type_hint(right, left.data_type())?
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
