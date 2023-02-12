mod filter;
mod hash_aggregator;
mod inmem_scan;
mod limit;
mod projection;
mod sort;

use crate::{errors::DBResult, row::Row, tables::RelationSchema};

pub use filter::Filter;
pub use hash_aggregator::HashAggregator;
pub use inmem_scan::InMemTableScan;
pub use limit::Limit;
pub use projection::Projection;
pub use sort::Sort;

pub trait PhysicalPlan {
    /// Setup this plan node, e.g. prepare some resources etc.
    fn setup(&mut self) -> DBResult<()>;
    /// Acting like an iterator to get the next now if present
    fn next(&mut self) -> DBResult<Option<Row<'static>>>;
    /// Return the schema/shape of the output rows.
    fn schema(&self) -> DBResult<RelationSchema>;
}
