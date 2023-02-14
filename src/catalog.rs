use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    tables::{inmem::InMemTable, Table, TableMeta},
    DBError, DBResult,
};

#[derive(Default)]
pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, Arc<RwLock<Box<dyn Table>>>>>>,
}

impl Catalog {
    pub fn new() -> Self {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        Self { tables }
    }

    pub fn try_get_table(
        &self,
        table_name: &str,
    ) -> DBResult<Arc<RwLock<Box<dyn Table>>>> {
        self.get_or_create_table(table_name).and_then(|opt_tbl| {
            opt_tbl.ok_or(DBError::TableNotFound(table_name.to_string()))
        })
    }

    fn get_or_create_table(
        &self,
        table_name: &str,
    ) -> DBResult<Option<Arc<RwLock<Box<dyn Table>>>>> {
        let opt_table = self
            .tables
            .read()
            .map_err(|_| {
                DBError::Unknown("Acceess read lock of tables failed!".to_string())
            })
            .map(|tables| tables.get(table_name).cloned())?;
        Ok(opt_table)
    }

    pub fn create_table(&self, name: String, meta: TableMeta) -> Result<(), DBError> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        let table = InMemTable::new(meta);
        tables_map.insert(name, Arc::new(RwLock::new(Box::new(table))));
        Ok(())
    }
}
