use crate::{expressions::Literal, row::Row, tables::RelationSchema, DBResult};

mod avg_agg;
mod sum_agg;
pub use avg_agg::AvgAgg;
pub use sum_agg::SumAgg;

pub trait Aggregator {
    /// generate a new result row that holds intermediate aggregation results
    fn initial_row(&self) -> DBResult<Row<'static>>;

    /// resolve aggregator expressions, consider make this private fn
    fn resolve_expr(&mut self, inbound_schema: &RelationSchema) -> DBResult<()>;

    /// process input row with current result row, update results into result row
    fn process(&self, input_row: &Row, result_row: &mut Row) -> DBResult<()>;

    /// calculate result based on result row
    fn result(&self, result_row: &Row) -> DBResult<Literal>;
}
