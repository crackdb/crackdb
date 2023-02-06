use crate::{
    expressions::Expression,
    interpreter::Interpreter,
    optimizer::{rules::ResolveExprRule, OptimizerContextForExpr},
    row::Row,
    tables::RelationSchema,
    DBError, DBResult,
};

/// AggregatingBuffer provides
pub struct AggregatingBuffer {
    buffer_schema: RelationSchema,
    aggregating_exprs: Vec<Expression>,
}

impl AggregatingBuffer {
    pub fn new(
        buffer_schema: RelationSchema,
        aggregating_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            buffer_schema,
            aggregating_exprs,
        }
    }
}

impl AggregatingBuffer {
    pub fn resolve_expr(
        &mut self,
        inbound_schema: &crate::tables::RelationSchema,
    ) -> DBResult<()> {
        let schema = RelationSchema::merge(inbound_schema, &self.buffer_schema);
        let context = OptimizerContextForExpr::new(schema);
        self.aggregating_exprs = self
            .aggregating_exprs
            .iter()
            .map(|expr| {
                expr.transform_bottom_up(
                    &context,
                    &mut ResolveExprRule::resolve_expression,
                )
                .and_then(|opt_resolved_expr| {
                    opt_resolved_expr.ok_or(DBError::InterpretingError(
                        "Cannot resolve aggregator expr".to_string(),
                    ))
                })
            })
            .collect::<DBResult<Vec<_>>>()?;
        Ok(())
    }

    pub fn process(&self, input_row: &Row, output_buffer: &mut Row) -> DBResult<()> {
        let outputs = {
            let target = Row::concat(input_row, output_buffer);
            self.aggregating_exprs
                .iter()
                .map(|expr| Interpreter::eval(expr, &target))
                .collect::<DBResult<Vec<_>>>()?
        };
        for (idx, output) in outputs.into_iter().enumerate() {
            output_buffer.update_field(idx, output)?;
        }
        Ok(())
    }
}
