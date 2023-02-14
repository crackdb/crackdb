use crate::{
    physical_plans::{InMemTableScan, PhysicalPlan},
    row::Row,
};

use super::{Table, TableMeta};

pub struct InMemTable {
    meta: TableMeta,
    data: Vec<Row<'static>>,
}

impl InMemTable {
    pub fn new(meta: TableMeta) -> Self {
        InMemTable {
            meta,
            data: Vec::new(),
        }
    }
}

impl Table for InMemTable {
    fn insert_data(&mut self, data: Vec<Row<'static>>) {
        self.data.extend(data)
    }

    fn read(&self) -> Box<dyn Iterator<Item = Row<'static>>> {
        Box::new(self.data.to_vec().into_iter())
    }

    fn get_table_meta(&self) -> TableMeta {
        self.meta.clone()
    }

    fn create_scan_op(&self) -> Box<dyn PhysicalPlan> {
        Box::new(InMemTableScan::new(
            self.data.clone(),
            self.meta.schema.clone(),
        ))
    }
}
