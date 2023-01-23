use std::sync::{Arc, RwLock};

use crate::{
    errors::DBError, errors::DBResult, expressions::Expression, row::Row,
    tables::InMemTable,
};

pub trait PhysicalPlan {
    /// Setup this plan node, e.g. prepare some resources etc.
    fn setup(&mut self) -> DBResult<()>;
    /// Acting like an iterator to get the next now if present
    fn next(&mut self) -> DBResult<Option<Row>>;
    /// Return the schema/shape of the output rows.
    fn schema(&self) -> DBResult<Vec<String>>;
}

pub struct InMemTableScan {
    table: Arc<RwLock<InMemTable>>,
    snapshot: Option<Vec<Row>>,
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

    fn next(&mut self) -> DBResult<Option<Row>> {
        let next = self
            .snapshot
            .as_ref()
            .filter(|rows| rows.len() > self.next)
            .map(|rows| rows[self.next].clone());
        self.next += 1;
        Ok(next)
    }

    fn schema(&self) -> DBResult<Vec<String>> {
        let table = self.table.as_ref().read().map_err(|_e| {
            DBError::Unknown("Access read lock of table failed!".to_string())
        })?;
        Ok(table.headers())
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

    fn next(&mut self) -> DBResult<Option<Row>> {
        while let Some(row) = self.child.next()? {
            if self.expression.eval(&row)?.as_bool()? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn schema(&self) -> DBResult<Vec<String>> {
        self.child.schema()
    }
}
