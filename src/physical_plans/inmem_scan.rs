use std::sync::{Arc, RwLock};

use crate::{
    row::Row,
    tables::{InMemTable, RelationSchema},
    DBError, DBResult,
};

use super::PhysicalPlan;

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
