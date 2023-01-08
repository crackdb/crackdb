use std::sync::{Arc, RwLock};

use sqlparser::ast::ColumnDef;

pub struct InMemTable {
    name: String,
    columns: Vec<ColumnDef>,
    data: Arc<RwLock<Vec<Row>>>,
}

pub struct Row {
    fields: Vec<i32>,
}

impl Row {
    pub fn new(fields: Vec<i32>) -> Self {
        Row { fields }
    }
}

impl InMemTable {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        InMemTable {
            name,
            columns,
            data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn data(&self) -> &RwLock<Vec<Row>> {
        Arc::as_ref(&self.data)
    }
}
