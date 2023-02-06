use crate::{
    data_types::DataType,
    expressions::Expression,
    logical_plans::LogicalPlan,
    optimizer::{OptimizerContext, OptimizerContextForExpr},
    DBResult,
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
        node.transform_bottom_up(context, &mut Self::resolve_expression)
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
                            .get_field_at(idx)
                            .unwrap()
                            .data_type()
                            .clone(),
                    })),
                    None => Ok(None),
                }
            }
            Expression::FieldRef {
                name,
                data_type,
                index,
            } if *data_type == DataType::Unknown => {
                match context.schema().get_field_at(*index) {
                    Some(f) if *(f.data_type()) != DataType::Unknown => {
                        Ok(Some(Expression::FieldRef {
                            name: name.to_string(),
                            index: *index,
                            data_type: f.data_type().clone(),
                        }))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}
