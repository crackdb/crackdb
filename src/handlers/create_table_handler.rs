use std::sync::{Arc, RwLock};

use sqlparser::ast::Statement;

use crate::{
    data_types::DataType,
    tables::{FieldInfo, RelationSchema, TableMeta},
    Catalog, DBError, ResultSet,
};

use super::QueryHandler;

pub struct CreateTableHandler {
    catalog: Arc<RwLock<Catalog>>,
}

impl QueryHandler for CreateTableHandler {
    fn handle(
        &self,
        statement: sqlparser::ast::Statement,
    ) -> crate::DBResult<crate::ResultSet> {
        match statement {
            Statement::CreateTable {
                or_replace: _,
                temporary: _,
                external: _,
                global: _,
                if_not_exists: _,
                name,
                columns,
                constraints: _,
                hive_distribution: _,
                hive_formats: _,
                table_properties: _,
                with_options: _,
                file_format: _,
                location: _,
                query: _,
                without_rowid: _,
                like: _,
                clone: _,
                engine: _,
                default_charset: _,
                collation: _,
                on_commit: _,
                on_cluster: _,
                ..
            } => {
                self.create_table(name, columns)?;
                Ok(ResultSet::empty())
            }
            _ => Err(DBError::Unknown("should never happen!".to_string())),
        }
    }
}

impl CreateTableHandler {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    /// create a table in CrackDB
    /// simply create a InMemTable and register it into a inmem map keyed by the table name
    fn create_table(
        &self,
        name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::ColumnDef>,
    ) -> Result<(), DBError> {
        // TODO: validate against unsupported data types
        let fields = columns
            .into_iter()
            .map(|c| FieldInfo::new(c.name.to_string(), DataType::from(c.data_type)))
            .collect();
        let schema = RelationSchema::new(fields);
        let meta = TableMeta::new(schema);
        RwLock::read(&self.catalog)
            .map_err(|_e| {
                DBError::Unknown("access catalog read lock failed.".to_string())
            })?
            .create_table(name.to_string(), meta)
    }
}
