use sqlparser::{
    ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor, Value, Values},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    ParserError(String),
    TableNotFound(String),
    Unknown(String),
}

pub type DBResult<T> = Result<T, DBError>;

impl From<ParserError> for DBError {
    fn from(e: ParserError) -> Self {
        DBError::ParserError(e.to_string())
    }
}
