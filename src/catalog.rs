use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    tables::{csv::CsvTable, inmem::InMemTable, Table, TableMeta},
    DBError, DBResult,
};

type CatalogTable = Arc<RwLock<Box<dyn Table>>>;
#[derive(Default)]
pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, CatalogTable>>>,
}

impl Catalog {
    pub fn new() -> Self {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        Self { tables }
    }

    pub fn try_get_table(&self, table_name: &str) -> DBResult<CatalogTable> {
        self.get_or_create_table(table_name).and_then(|opt_tbl| {
            opt_tbl.ok_or(DBError::TableNotFound(table_name.to_string()))
        })
    }

    fn get_or_create_table(&self, table_name: &str) -> DBResult<Option<CatalogTable>> {
        let opt_table = self
            .tables
            .read()
            .map_err(|_| {
                DBError::Unknown("Acceess read lock of tables failed!".to_string())
            })
            .map(|tables| tables.get(table_name).cloned())?;
        if opt_table.is_none() && table_name.to_lowercase().ends_with(".csv") {
            // read csv ad-hoc from given path
            let csv_table = CsvTable::new(table_name.to_owned())?;
            self.add_new_table(table_name.to_owned(), Box::new(csv_table))?;
            return self.get_or_create_table(table_name);
        }
        Ok(opt_table)
    }

    fn add_new_table(&self, name: String, table: Box<dyn Table>) -> DBResult<()> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        tables_map.insert(name, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn create_table(&self, name: String, meta: TableMeta) -> DBResult<()> {
        let table = InMemTable::new(meta);
        self.add_new_table(name, Box::new(table))
    }
}
