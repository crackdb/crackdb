use crate::{
    expressions::{Expression, Literal},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBResult,
};

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

/// Build the schema for Aggregator plan node.
///
/// Append aggregators after groupings to make the scehma stable, since aggregators
/// might change during optimization
pub fn aggregator_schema(
    groupings: &[Expression],
    aggregators: &[Expression],
) -> RelationSchema {
    let fields = groupings
        .iter()
        .chain(aggregators.iter())
        .map(|expr| FieldInfo::new(expr.to_string(), expr.data_type()))
        .collect();
    RelationSchema::new(fields)
}
