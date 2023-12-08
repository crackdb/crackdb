use std::{num::{ParseFloatError, ParseIntError}, fmt::Display};

use sqlparser::parser::ParserError;

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    ParserError(String),
    TableNotFound(String),
    InterpretingError(String),
    Unknown(String),
    StorageEngine(String),
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::ParserError(msg) => write!(f, "ParserError: {}", msg),
            DBError::TableNotFound(msg) => write!(f, "TableNoteFound: {}", msg),
            DBError::InterpretingError(msg) => write!(f, "InterpretingError: {}", msg),
            DBError::Unknown(msg) => write!(f, "Unknown: {}", msg),
            DBError::StorageEngine(msg) => write!(f, "StorageEngineError: {}", msg),
        }
    }
}

impl std::error::Error for DBError {
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

impl DBError {
    pub fn should_never_happen() -> DBError {
        DBError::Unknown("should never happen".to_owned())
    }
}
