use crate::{
    data_types::DataType,
    expressions::{BinaryOp, Expression, Literal},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBError, DBResult,
};

use super::{aggregating_buffer::AggregatingBuffer, Aggregator};

const FIELD_AVG_SUM: &str = "avg_sum";
const FIELD_AVG_COUNT: &str = "avg_count";

pub struct AvgAgg {
    agg_buffer: AggregatingBuffer,
}

impl AvgAgg {
    pub fn new(input: &Expression) -> DBResult<Self> {
        let sum_expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(input.clone()),
            right: Box::new(Expression::UnResolvedFieldRef(FIELD_AVG_SUM.to_owned())),
        };
        let count_expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(Expression::Literal(Literal::UInt64(1))),
            right: Box::new(Expression::UnResolvedFieldRef(FIELD_AVG_COUNT.to_owned())),
        };
        let aggregating_exprs = vec![sum_expr, count_expr];
        let buffer_schema = RelationSchema::new(vec![
            FieldInfo::new(FIELD_AVG_SUM.to_owned(), DataType::Float64),
            FieldInfo::new(FIELD_AVG_COUNT.to_owned(), DataType::UInt64),
        ]);
        let agg_buffer = AggregatingBuffer::new(buffer_schema, aggregating_exprs);
        Ok(Self { agg_buffer })
    }
}

impl Aggregator for AvgAgg {
    fn initial_row(&self) -> DBResult<Row<'static>> {
        Ok(Row::new(vec![Literal::Float64(0.0), Literal::UInt64(0)]))
    }

    fn resolve_expr(&mut self, inbound_schema: &RelationSchema) -> DBResult<()> {
        self.agg_buffer.resolve_expr(inbound_schema)
    }

    fn process(&self, input_row: &Row, output_buffer: &mut Row) -> DBResult<()> {
        self.agg_buffer.process(input_row, output_buffer)
    }

    fn result(&self, output_buffer: &Row) -> DBResult<Literal> {
        match (output_buffer.get_field(0)?, output_buffer.get_field(1)?) {
            (Literal::Float64(sum), Literal::UInt64(count)) => {
                Ok(Literal::Float64(sum / (count as f64)))
            }
            _ => Err(DBError::Unknown("should never happen.".to_owned())),
        }
    }
}
