use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    tables::{InMemTable, TableMeta},
    DBError, DBResult,
};

#[derive(Default)]
pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, Arc<RwLock<InMemTable>>>>>,
}

impl Catalog {
    pub fn new() -> Self {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        Self { tables }
    }
    pub fn try_get_table(&self, table_name: &str) -> DBResult<Arc<RwLock<InMemTable>>> {
        self.tables
            .read()
            .map_err(|_| {
                DBError::Unknown("Acceess read lock of tables failed!".to_string())
            })
            .and_then(|tables| {
                tables
                    .get(table_name)
                    .cloned()
                    .ok_or_else(|| DBError::TableNotFound(table_name.to_string()))
            })
    }

    pub fn create_table(&self, name: String, meta: TableMeta) -> Result<(), DBError> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        tables_map.insert(name, Arc::new(RwLock::new(InMemTable::new(meta))));
        Ok(())
    }
}
