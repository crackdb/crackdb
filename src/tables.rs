use sqlparser::ast::ColumnDef;

use crate::{expressions::Literal, DBError, DBResult};

pub struct InMemTable {
    columns: Vec<ColumnDef>,
    data: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub fields: Vec<i32>,
}

impl Row {
    pub fn new(fields: Vec<i32>) -> Self {
        Row { fields }
    }

    pub fn get_field(&self, index: usize) -> DBResult<Literal> {
        match self.fields.get(index) {
            Some(val) => Ok(Literal::Int(*val)),
            None if index >= self.fields.len() => {
                Err(DBError::Unknown("Index out of bound.".to_string()))
            }
            None => Err(DBError::Unknown("Value not exists.".to_string())),
        }
    }
}

impl InMemTable {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        InMemTable {
            columns,
            data: Vec::new(),
        }
    }

    pub fn insert_data(&mut self, data: Vec<Row>) {
        self.data.extend(data)
    }

    // FIXME: avoid copy data for reading purpose
    pub fn read(&self) -> Vec<Row> {
        self.data.to_vec()
    }

    pub fn headers(&self) -> Vec<String> {
        self.columns.iter().map(|f| f.name.to_string()).collect()
    }
}
