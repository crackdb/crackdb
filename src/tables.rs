use crate::data_types::DataType;
use crate::row::Row;

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

    pub fn insert_data(&mut self, data: Vec<Row<'static>>) {
        self.data.extend(data)
    }

    // FIXME: avoid copy data for reading purpose
    pub fn read(&self) -> Vec<Row<'static>> {
        self.data.to_vec()
    }

    pub fn headers(&self) -> Vec<String> {
        self.meta
            .schema
            .fields
            .iter()
            .map(|f| f.name.to_string())
            .collect()
    }

    pub fn get_table_meta(&self) -> TableMeta {
        self.meta.clone()
    }
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    schema: RelationSchema,
}

impl TableMeta {
    pub fn new(schema: RelationSchema) -> Self {
        TableMeta { schema }
    }

    pub fn get_schema(&self) -> &RelationSchema {
        &self.schema
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RelationSchema {
    fields: Vec<FieldInfo>,
}

impl RelationSchema {
    pub fn new(fields: Vec<FieldInfo>) -> Self {
        RelationSchema { fields }
    }

    pub fn get_fields(&self) -> &Vec<FieldInfo> {
        &self.fields
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldInfo {
    name: String,
    data_type: DataType,
}

impl FieldInfo {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}
