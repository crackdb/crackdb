use crate::{row::Row, tables::RelationSchema, DBResult};

use super::PhysicalPlan;

pub struct InMemTableScan {
    data: Vec<Row<'static>>,
    schema: RelationSchema,
    next: usize,
}

impl InMemTableScan {
    pub fn new(data: Vec<Row<'static>>, schema: RelationSchema) -> Self {
        Self {
            data,
            schema,
            next: 0,
        }
    }
}

impl PhysicalPlan for InMemTableScan {
    // FIXME: avoid copying data for snapshot reading
    fn setup(&mut self) -> DBResult<()> {
        Ok(())
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        if self.data.len() > self.next {
            self.next += 1;
            Ok(Some(self.data[self.next - 1].clone()))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        Ok(self.schema.clone())
    }
}
