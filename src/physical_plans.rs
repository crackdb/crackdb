mod hash_aggregator;
mod projection;

use std::sync::{Arc, RwLock};

use crate::{
    errors::DBError,
    errors::DBResult,
    expressions::Expression,
    interpreter::Interpreter,
    row::Row,
    tables::{InMemTable, RelationSchema},
};

pub use hash_aggregator::HashAggregator;
pub use projection::Projection;

pub trait PhysicalPlan {
    /// Setup this plan node, e.g. prepare some resources etc.
    fn setup(&mut self) -> DBResult<()>;
    /// Acting like an iterator to get the next now if present
    fn next(&mut self) -> DBResult<Option<Row<'static>>>;
    /// Return the schema/shape of the output rows.
    fn schema(&self) -> DBResult<RelationSchema>;
}

pub struct InMemTableScan {
    table: Arc<RwLock<InMemTable>>,
    snapshot: Option<Vec<Row<'static>>>,
    next: usize,
}

impl InMemTableScan {
    pub fn new(table: Arc<RwLock<InMemTable>>) -> Self {
        Self {
            table,
            snapshot: None,
            next: 0,
        }
    }
}

impl PhysicalPlan for InMemTableScan {
    // FIXME: avoid copying data for snapshot reading
    fn setup(&mut self) -> DBResult<()> {
        let table = self.table.as_ref().read().map_err(|_e| {
            DBError::Unknown("Access read lock of table failed!".to_string())
        })?;
        self.snapshot = Some(table.read());
        Ok(())
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        let next = self
            .snapshot
            .as_ref()
            .filter(|rows| rows.len() > self.next)
            .map(|rows| rows[self.next].clone());
        self.next += 1;
        Ok(next)
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        let table = self.table.as_ref().read().map_err(|_e| {
            DBError::Unknown("Access read lock of table failed!".to_string())
        })?;
        Ok(table.get_table_meta().get_schema().clone())
    }
}

pub struct Filter {
    expression: Expression,
    child: Box<dyn PhysicalPlan>,
}

impl Filter {
    pub fn new(expression: Expression, child: Box<dyn PhysicalPlan>) -> Self {
        Self { expression, child }
    }
}

impl PhysicalPlan for Filter {
    fn setup(&mut self) -> DBResult<()> {
        self.child.setup()
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        while let Some(row) = self.child.next()? {
            if Interpreter::eval(&self.expression, &row)?.as_bool()? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        self.child.schema()
    }
}
