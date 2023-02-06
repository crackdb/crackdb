use crate::{
    data_types::DataType,
    expressions::{BinaryOp, Expression, Literal},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBResult,
};

use super::{aggregating_buffer::AggregatingBuffer, Aggregator};

const FIELD_COUNT: &str = "count_agg_count";

pub struct CountAgg {
    agg_buffer: AggregatingBuffer,
}

impl CountAgg {
    pub fn new(_arg: &Expression) -> DBResult<Self> {
        let expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(Expression::UnResolvedFieldRef(FIELD_COUNT.to_string())),
            right: Box::new(Expression::Literal(Literal::UInt64(1))),
        };
        let aggregating_exprs = vec![expr];
        let buffer_schema = RelationSchema::new(vec![FieldInfo::new(
            FIELD_COUNT.to_owned(),
            DataType::UInt64,
        )]);
        let agg_buffer = AggregatingBuffer::new(buffer_schema, aggregating_exprs);
        Ok(Self { agg_buffer })
    }
}
impl Aggregator for CountAgg {
    fn initial_row(&self) -> DBResult<Row<'static>> {
        // TODO: consider self.agg_buffer.buffer_schema.row(literals) to get the row
        Ok(Row::new(vec![Literal::UInt64(0)]))
    }

    fn resolve_expr(&mut self, inbound_schema: &RelationSchema) -> DBResult<()> {
        self.agg_buffer.resolve_expr(inbound_schema)
    }

    fn process(&self, input_row: &Row, output_buffer: &mut Row) -> DBResult<()> {
        self.agg_buffer.process(input_row, output_buffer)
    }

    fn result(&self, output_buffer: &Row) -> DBResult<Literal> {
        output_buffer.get_field(0)
    }
}
