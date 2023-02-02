use std::num::{ParseFloatError, ParseIntError};

use sqlparser::parser::ParserError;

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    ParserError(String),
    TableNotFound(String),
    InterpretingError(String),
    Unknown(String),
}

pub type DBResult<T> = Result<T, DBError>;

impl From<ParserError> for DBError {
    fn from(e: ParserError) -> Self {
        DBError::ParserError(e.to_string())
    }
}

impl From<ParseFloatError> for DBError {
    fn from(e: ParseFloatError) -> Self {
        DBError::ParserError(format!("cannot parse float number: {e}"))
    }
}

impl From<ParseIntError> for DBError {
    fn from(e: ParseIntError) -> Self {
        DBError::ParserError(format!("cannot parse int number: {e}"))
    }
}
