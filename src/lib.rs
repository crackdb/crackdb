pub mod catalog;
mod errors;
pub mod expressions;
pub mod handlers;
pub mod logical_plans;
pub mod pysical_plans;
pub mod tables;
use catalog::Catalog;
pub use errors::*;
use handlers::{CreateTableHandler, InsertHandler, QueryHandler, SelectHandler};

pub mod data_types;
pub mod row;

pub mod optimizer;

use std::sync::{Arc, RwLock};

use crate::row::Row;

use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

pub struct CrackDB {
    select_handler: Box<dyn QueryHandler>,
    insert_handler: Box<dyn QueryHandler>,
    create_table_handler: Box<dyn QueryHandler>,
}

impl Default for CrackDB {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq)]
pub struct ResultSet {
    pub headers: Vec<String>,
    pub rows: Vec<Row>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            headers: vec![],
            rows: vec![],
        }
    }
    pub fn new(headers: Vec<String>, rows: Vec<Row>) -> Self {
        ResultSet { headers, rows }
    }
}

impl CrackDB {
    pub fn new() -> Self {
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let select_handler = Box::new(SelectHandler::new(Arc::clone(&catalog)));
        let insert_handler = Box::new(InsertHandler::new(Arc::clone(&catalog)));
        let create_table_handler =
            Box::new(CreateTableHandler::new(Arc::clone(&catalog)));
        CrackDB {
            select_handler,
            insert_handler,
            create_table_handler,
        }
    }

    pub fn execute(&self, query: &str) -> Result<ResultSet, DBError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, query)?;
        if statements.len() != 1 {
            return Err(DBError::ParserError(
                "only single query statement is supported.".to_string(),
            ));
        }
        let statement = Iterator::next(&mut statements.into_iter()).unwrap();
        // TODO: log the AST in debug level
        // println!("AST: {:?}", statement);
        // TODO: warn any present but unused nodes in AST
        // TODO: check and validate AST
        match statement {
            Statement::CreateTable { .. } => self.create_table_handler.handle(statement),
            Statement::Insert { .. } => self.insert_handler.handle(statement),
            Statement::Query(..) => self.select_handler.handle(statement),
            _ => Err(DBError::Unknown("statement not supported.".to_string())),
        }
    }
}
