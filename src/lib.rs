pub mod tables;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::tables::{InMemTable, Row};

use sqlparser::{
    ast::{Expr, SetExpr, Statement, Value, Values},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};

pub struct CrackDB {
    tables: Arc<RwLock<HashMap<String, InMemTable>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    ParserError(String),
    TableNotFound(String),
    Unknown(String),
}

impl From<ParserError> for DBError {
    fn from(e: ParserError) -> Self {
        DBError::ParserError(e.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResultSet {
    pub headers: Vec<String>,
    // TODO: only i32 colums are supported now
    pub rows: Vec<Vec<i32>>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            headers: vec![],
            rows: vec![],
        }
    }
    pub fn new(headers: Vec<String>, rows: Vec<Vec<i32>>) -> Self {
        ResultSet { headers, rows }
    }
}

impl CrackDB {
    pub fn execute(&self, query: &str) -> Result<ResultSet, DBError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, query)?;
        if statements.len() != 1 {
            return Err(DBError::ParserError(
                "only single query statement is supported.".to_string(),
            ));
        }
        let statement = Iterator::next(&mut statements.into_iter()).unwrap();
        println!("AST: {:?}", statement);
        // TODO: warn any present but unused nodes in AST
        // TODO: check and validate AST
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
            } => {
                self.create_table(name, columns)?;
                Ok(ResultSet::empty())
            }
            Statement::Insert {
                or: _,
                into: _,
                table_name,
                columns,
                overwrite: _,
                source,
                partitioned: _,
                after_columns: _,
                table: _,
                on: _,
                returning: _,
            } => {
                self.process_insert(table_name, columns, *source)?;
                Ok(ResultSet::empty())
            }
            Statement::Query(..) => panic!("query not implemented yet!"),
            _ => return Err(DBError::Unknown("statement not supported.".to_string())),
        }
    }

    pub fn new() -> Self {
        CrackDB {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// create a table in CrackDB
    /// simply create a InMemTable and register it into a inmem map keyed by the table name
    fn create_table(
        &self,
        name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::ColumnDef>,
    ) -> Result<(), DBError> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        tables_map.insert(name.to_string(), InMemTable::new(name.to_string(), columns));
        Ok(())
    }

    fn process_insert(
        &self,
        table_name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::Ident>,
        source: sqlparser::ast::Query,
    ) -> Result<(), DBError> {
        assert!(
            columns.is_empty(),
            "insert into with columns not supported by now!"
        );

        // FIXME: assuming all inser data are i32 without consulting the table schema
        let rows_to_insert: Vec<Row> = match *source.body {
            SetExpr::Values(Values {
                rows,
                explicit_row: _,
            }) => rows
                .into_iter()
                .map(|r| {
                    let fields: Vec<i32> = r
                        .into_iter()
                        .map(|field| match field {
                            Expr::Value(Value::Number(v, _)) => v.parse::<i32>().unwrap(),
                            Expr::Value(_) => panic!("unsupported insert value type!"),
                            _ => panic!("unsupported insert value!"),
                        })
                        .collect();
                    Row::new(fields)
                })
                .collect(),
            _ => panic!("unsupported insert statement type!"),
        };

        let tables_map = RwLock::read(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access read lock of DB tables failed!".to_string())
        })?;
        match tables_map.get(table_name.to_string().as_str()) {
            Some(table) => {
                let mut rows = RwLock::write(table.data()).map_err(|_| {
                    DBError::Unknown("Access write lock of table failed!".to_string())
                })?;
                rows.extend(rows_to_insert);
                Ok(())
            }
            None => Err(DBError::TableNotFound(table_name.to_string())),
        }
    }
}
