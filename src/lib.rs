mod aggregators;
pub mod catalog;
mod errors;
pub mod expressions;
mod functions;
pub mod handlers;
pub mod interpreter;
pub mod logical_plans;
mod parser;
pub mod physical_plans;
pub mod tables;
use catalog::Catalog;
pub use errors::*;
use expressions::Literal;
use handlers::{CreateTableHandler, InsertHandler, QueryHandler, SelectHandler};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize,
};
use tables::RelationSchema;

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
    pub schema: RelationSchema,
    pub rows: Vec<Row<'static>>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            schema: RelationSchema::new(vec![]),
            rows: vec![],
        }
    }
    pub fn new(schema: RelationSchema, rows: Vec<Row<'static>>) -> Self {
        ResultSet { schema, rows }
    }
}

pub struct RowWithSchema<'a> {
    pub row: &'a Row<'static>,
    pub schema: &'a RelationSchema,
}

impl<'a> RowWithSchema<'a> {
    pub fn new(schema: &'a RelationSchema, row: &'a Row<'static>) -> Self {
        Self { row, schema }
    }
}

impl Serialize for ResultSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.rows.len()))?;
        for r in self.rows.iter() {
            seq.serialize_element(&RowWithSchema::new(&self.schema, r))?;
        }
        seq.end()
    }
}

impl<'a> Serialize for RowWithSchema<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.schema.num_fields()))?;
        for (idx, field) in self.schema.get_fields().iter().enumerate() {
            let value = self.row.get_field(idx).unwrap_or(Literal::Null);
            map.serialize_entry(field.name(), &value)?;
        }
        map.end()
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
        // println!("AST: {statement:?}");
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
