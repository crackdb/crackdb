use std::fmt::Display;

use crate::{expressions::Literal, DBError, DBResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Boolean,
    DateTime,
    Unknown,
}

impl DataType {
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
        )
    }

    pub fn zero(&self) -> DBResult<Literal> {
        match self {
            DataType::UInt8 => Ok(Literal::UInt8(0)),
            DataType::UInt16 => Ok(Literal::UInt16(0)),
            DataType::UInt32 => Ok(Literal::UInt32(0)),
            DataType::UInt64 => Ok(Literal::UInt64(0)),
            DataType::Int8 => Ok(Literal::Int8(0)),
            DataType::Int16 => Ok(Literal::Int16(0)),
            DataType::Int32 => Ok(Literal::Int32(0)),
            DataType::Int64 => Ok(Literal::Int64(0)),
            DataType::Float32 => Ok(Literal::Float32(0.0)),
            DataType::Float64 => Ok(Literal::Float64(0.0)),
            _ => Err(DBError::Unknown(format!("zero not supported for {self}"))),
        }
    }

    pub fn max_value(&self) -> DBResult<Literal> {
        match self {
            DataType::UInt8 => Ok(Literal::UInt8(u8::MAX)),
            DataType::UInt16 => Ok(Literal::UInt16(u16::MAX)),
            DataType::UInt32 => Ok(Literal::UInt32(u32::MAX)),
            DataType::UInt64 => Ok(Literal::UInt64(u64::MAX)),
            DataType::Int8 => Ok(Literal::Int8(i8::MAX)),
            DataType::Int16 => Ok(Literal::Int16(i16::MAX)),
            DataType::Int32 => Ok(Literal::Int32(i32::MAX)),
            DataType::Int64 => Ok(Literal::Int64(i64::MAX)),
            DataType::Float32 => Ok(Literal::Float32(f32::MAX)),
            DataType::Float64 => Ok(Literal::Float64(f64::MAX)),
            _ => Err(DBError::Unknown(format!(
                "max_value not supported for {self}"
            ))),
        }
    }

    pub fn min_value(&self) -> DBResult<Literal> {
        match self {
            DataType::UInt8 => Ok(Literal::UInt8(u8::MIN)),
            DataType::UInt16 => Ok(Literal::UInt16(u16::MIN)),
            DataType::UInt32 => Ok(Literal::UInt32(u32::MIN)),
            DataType::UInt64 => Ok(Literal::UInt64(u64::MIN)),
            DataType::Int8 => Ok(Literal::Int8(i8::MIN)),
            DataType::Int16 => Ok(Literal::Int16(i16::MIN)),
            DataType::Int32 => Ok(Literal::Int32(i32::MIN)),
            DataType::Int64 => Ok(Literal::Int64(i64::MIN)),
            DataType::Float32 => Ok(Literal::Float32(f32::MIN)),
            DataType::Float64 => Ok(Literal::Float64(f64::MIN)),
            _ => Err(DBError::Unknown(format!(
                "min_value not supported for {self}"
            ))),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<sqlparser::ast::DataType> for DataType {
    fn from(dt: sqlparser::ast::DataType) -> Self {
        match dt {
            sqlparser::ast::DataType::Character(_) => DataType::Unknown,
            sqlparser::ast::DataType::Char(_) => DataType::Unknown,
            sqlparser::ast::DataType::CharacterVarying(_) => DataType::Unknown,
            sqlparser::ast::DataType::CharVarying(_) => DataType::Unknown,
            sqlparser::ast::DataType::Varchar(_) => DataType::Unknown,
            sqlparser::ast::DataType::Nvarchar(_) => DataType::Unknown,
            sqlparser::ast::DataType::Uuid => DataType::Unknown,
            sqlparser::ast::DataType::CharacterLargeObject(_) => DataType::Unknown,
            sqlparser::ast::DataType::CharLargeObject(_) => DataType::Unknown,
            sqlparser::ast::DataType::Clob(_) => DataType::Unknown,
            sqlparser::ast::DataType::Binary(_) => DataType::Unknown,
            sqlparser::ast::DataType::Varbinary(_) => DataType::Unknown,
            sqlparser::ast::DataType::Blob(_) => DataType::Unknown,
            sqlparser::ast::DataType::Numeric(_) => DataType::Unknown,
            sqlparser::ast::DataType::Decimal(_) => DataType::Unknown,
            sqlparser::ast::DataType::Dec(_) => DataType::Unknown,
            sqlparser::ast::DataType::Float(_) => DataType::Float32,
            sqlparser::ast::DataType::TinyInt(_) => DataType::Int8,
            sqlparser::ast::DataType::UnsignedTinyInt(_) => DataType::UInt8,
            sqlparser::ast::DataType::SmallInt(_) => DataType::Int16,
            sqlparser::ast::DataType::UnsignedSmallInt(_) => DataType::UInt16,
            sqlparser::ast::DataType::MediumInt(_) => DataType::Unknown,
            sqlparser::ast::DataType::UnsignedMediumInt(_) => DataType::Unknown,
            sqlparser::ast::DataType::Int(_) => DataType::Int32,
            sqlparser::ast::DataType::Integer(_) => DataType::Int32,
            sqlparser::ast::DataType::UnsignedInt(_) => DataType::UInt32,
            sqlparser::ast::DataType::UnsignedInteger(_) => DataType::UInt32,
            sqlparser::ast::DataType::BigInt(_) => DataType::Int64,
            sqlparser::ast::DataType::UnsignedBigInt(_) => DataType::UInt64,
            sqlparser::ast::DataType::Real => DataType::Unknown,
            sqlparser::ast::DataType::Double => DataType::Float64,
            sqlparser::ast::DataType::DoublePrecision => DataType::Float64,
            sqlparser::ast::DataType::Boolean => DataType::Boolean,
            sqlparser::ast::DataType::Date => DataType::Unknown,
            sqlparser::ast::DataType::Time(_, _) => DataType::Unknown,
            sqlparser::ast::DataType::Datetime(_) => DataType::DateTime,
            sqlparser::ast::DataType::Timestamp(_, _) => DataType::Unknown,
            sqlparser::ast::DataType::Interval => DataType::Unknown,
            sqlparser::ast::DataType::Regclass => DataType::Unknown,
            sqlparser::ast::DataType::Text => DataType::String,
            sqlparser::ast::DataType::String => DataType::String,
            sqlparser::ast::DataType::Bytea => DataType::Unknown,
            sqlparser::ast::DataType::Custom(_, _) => DataType::Unknown,
            sqlparser::ast::DataType::Array(_) => DataType::Unknown,
            sqlparser::ast::DataType::Enum(_) => DataType::Unknown,
            sqlparser::ast::DataType::Set(_) => DataType::Unknown,
        }
    }
}
