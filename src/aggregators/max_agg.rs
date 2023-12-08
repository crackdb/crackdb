use crate::{
    data_types::DataType,
    expressions::{BinaryOp, Expression},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBResult,
};

use super::{aggregating_buffer::AggregatingBuffer, Aggregator};

const FIELD_MAX: &str = "max_agg_max";

pub struct MaxAgg {
    agg_buffer: AggregatingBuffer,
    data_type: DataType,
}

impl MaxAgg {
    pub fn new(arg: &Expression) -> DBResult<Self> {
        let expr = Expression::BinaryOp {
            op: BinaryOp::Max,
            left: Box::new(Expression::UnResolvedFieldRef(FIELD_MAX.to_string())),
            right: Box::new(arg.clone()),
        };
        let data_type = arg.data_type();
        let aggregating_exprs = vec![expr];
        let buffer_schema = RelationSchema::new(vec![FieldInfo::new(
            FIELD_MAX.to_owned(),
            data_type.clone(),
        )]);
        let agg_buffer = AggregatingBuffer::new(buffer_schema, aggregating_exprs);
        Ok(Self {
            agg_buffer,
            data_type,
        })
    }
}

impl Aggregator for MaxAgg {
    fn initial_row(&self) -> DBResult<Row<'static>> {
        // TODO: consider self.agg_buffer.buffer_schema.row(literals) to get the row
        Ok(Row::new(vec![self.data_type.min_value()?]))
    }

    fn resolve_expr(&mut self, inbound_schema: &RelationSchema) -> DBResult<()> {
        self.agg_buffer.resolve_expr(inbound_schema)
    }

    fn process(&self, input_row: &Row, output_buffer: &mut Row) -> DBResult<()> {
        self.agg_buffer.process(input_row, output_buffer)
    }

    fn result(&self, output_buffer: &Row) -> DBResult<crate::expressions::Literal> {
        output_buffer.get_field(0)
    }
}
