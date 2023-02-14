pub mod inmem;

use crate::data_types::DataType;
use crate::physical_plans::PhysicalPlan;
use crate::row::Row;

pub trait Table {
    fn insert_data(&mut self, data: Vec<Row<'static>>);

    fn read(&self) -> Box<dyn Iterator<Item = Row<'static>>>;

    fn get_table_meta(&self) -> TableMeta;

    fn create_scan_op(&self) -> Box<dyn PhysicalPlan>;
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

    pub fn get_field_at(&self, idx: usize) -> Option<&FieldInfo> {
        self.fields.get(idx)
    }

    pub(crate) fn empty() -> RelationSchema {
        RelationSchema::new(vec![])
    }

    pub(crate) fn merge(left: &RelationSchema, right: &RelationSchema) -> RelationSchema {
        // TODO: fields deduplication
        let new_fields = left
            .get_fields()
            .iter()
            .chain(right.get_fields().iter())
            .cloned()
            .collect();
        RelationSchema::new(new_fields)
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
