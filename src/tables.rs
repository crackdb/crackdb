use sqlparser::ast::ColumnDef;

pub struct InMemTable {
    name: String,
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
}

impl InMemTable {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        InMemTable {
            name,
            columns,
            data: Vec::new(),
        }
    }

    pub fn insert_data(&mut self, data: Vec<Row>) {
        self.data.extend(data)
    }

    // FIXME: avoid copy data for reading purpose
    pub fn read(&self) -> Vec<Row> {
        self.data.iter().map(|r| r.clone()).collect()
    }

    pub fn headers(&self) -> Vec<String> {
        self.columns.iter().map(|f| f.name.to_string()).collect()
    }
}
