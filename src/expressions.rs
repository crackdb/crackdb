use crate::data_types::DataType;
use crate::optimizer::{OptimizerContextForExpr, OptimizerNode};
use crate::DBError;
use crate::{row::Row, DBResult};

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    UnResolvedFieldRef(String),
    FieldRef(usize, DataType),
    BooleanExpr(Box<BooleanExpr>),
}

impl OptimizerNode for Expression {
    type Context = OptimizerContextForExpr;
}

impl Expression {
    pub fn eval(&self, row: &Row) -> DBResult<Literal> {
        match self {
            Self::Literal(l) => Ok(l.clone()),
            Self::UnResolvedFieldRef(_field) => Err(crate::DBError::Unknown(
                "Trying evaluate an unresolved expression.".to_string(),
            )),
            Self::FieldRef(idx, _) => row.get_field(*idx),
            Self::BooleanExpr(ref bool_expr) => bool_expr.eval(row),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Expression::Literal(l) => l.data_type(),
            Expression::UnResolvedFieldRef(_) => DataType::Unknown,
            Expression::FieldRef(_, data_type) => data_type.clone(),
            Expression::BooleanExpr(_) => DataType::Boolean,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BooleanExpr {
    GT { left: Expression, right: Expression },
    GTE { left: Expression, right: Expression },
    EQ { left: Expression, right: Expression },
    LT { left: Expression, right: Expression },
    LTE { left: Expression, right: Expression },
}

impl BooleanExpr {
    pub fn eval(&self, row: &Row) -> DBResult<Literal> {
        match self {
            BooleanExpr::GT { left, right } => {
                match (left.eval(row)?, right.eval(row)?) {
                    (Literal::Int32(left), Literal::Int32(right)) => {
                        Ok(Literal::Bool(left > right))
                    }
                    _ => todo!(),
                }
            }
            BooleanExpr::GTE { left: _, right: _ } => todo!(),
            BooleanExpr::EQ { left, right } => {
                match (left.eval(row)?, right.eval(row)?) {
                    (Literal::Int32(v1), Literal::Int32(v2)) => {
                        Ok(Literal::Bool(v1 == v2))
                    }
                    _ => todo!(),
                }
            }
            BooleanExpr::LT { left: _, right: _ } => todo!(),
            BooleanExpr::LTE { left: _, right: _ } => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    UnResolvedNumber(String),
    UnResolvedString(String),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    DateTime(String),
    Null,
}

impl Literal {
    pub fn as_bool(&self) -> DBResult<bool> {
        match self {
            Literal::Bool(v) => Ok(*v),
            _ => Err(DBError::Unknown("Cannot convert int to bool.".to_string())),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Literal::UnResolvedNumber(_) => DataType::Unknown,
            Literal::UnResolvedString(_) => DataType::Unknown,
            Literal::UInt8(_) => DataType::UInt8,
            Literal::UInt16(_) => DataType::UInt16,
            Literal::UInt32(_) => DataType::UInt32,
            Literal::UInt64(_) => DataType::UInt64,
            Literal::Int8(_) => DataType::Int8,
            Literal::Int16(_) => DataType::Int16,
            Literal::Int32(_) => DataType::Int32,
            Literal::Int64(_) => DataType::Int64,
            Literal::Float32(_) => DataType::Float32,
            Literal::Float64(_) => DataType::Float64,
            Literal::Bool(_) => DataType::Boolean,
            Literal::String(_) => DataType::String,
            Literal::DateTime(_) => DataType::DateTime,
            Literal::Null => DataType::Unknown,
        }
    }

    /// cast literal to the given data type, or cast to a higher precesision data type in order to maintain precession if:
    /// 1. the literal already has higher precision than given data type
    /// 2. the literal will have higher precission after casting
    pub fn cast_or_maintain_precision(
        &self,
        data_type: DataType,
    ) -> DBResult<Option<Literal>> {
        match self {
            // for numbers:
            // 1. always cast to Float64 if number looks like float and data_type hint is int
            // 2. always cast to Int64 or UInt64 if the given data_type cannot satisfy the required precession when parsing
            Literal::UnResolvedNumber(v) => {
                if data_type.is_integer() && looks_like_float(v) {
                    let f = v.parse::<f64>()?;
                    Ok(Some(Literal::Float64(f)))
                } else {
                    parse_number(data_type, v)
                }
            }
            Literal::UnResolvedString(v) => match data_type {
                DataType::String => Ok(Some(Literal::String(v.to_string()))),
                DataType::DateTime => Ok(Some(Literal::DateTime(v.to_string()))),
                _ => Ok(None),
            },
            // TODO: add support for more castings
            _ => Ok(None),
        }
    }
}

/// parse string into given numeric data type
fn parse_number(data_type: DataType, v: &str) -> Result<Option<Literal>, DBError> {
    match data_type {
        DataType::UInt8 => {
            let u = v.parse::<u64>()?;
            if u >= u8::MIN as u64 && u <= u8::MAX as u64 {
                Ok(Some(Literal::UInt8(u as u8)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt16 => {
            let u = v.parse::<u64>()?;
            if u >= u16::MIN as u64 && u <= u16::MAX as u64 {
                Ok(Some(Literal::UInt16(u as u16)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt32 => {
            let u = v.parse::<u64>()?;
            if u >= u32::MIN as u64 && u <= u32::MAX as u64 {
                Ok(Some(Literal::UInt32(u as u32)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt64 => Ok(Some(Literal::UInt64(v.parse::<u64>()?))),
        DataType::Int8 => {
            let u = v.parse::<i64>()?;
            if u >= i8::MIN as i64 && u <= i8::MAX as i64 {
                Ok(Some(Literal::Int8(u as i8)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int16 => {
            let u = v.parse::<i64>()?;
            if u >= i16::MIN as i64 && u <= i16::MAX as i64 {
                Ok(Some(Literal::Int16(u as i16)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int32 => {
            let u = v.parse::<i64>()?;
            if u >= i32::MIN as i64 && u <= i32::MAX as i64 {
                Ok(Some(Literal::Int32(u as i32)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int64 => Ok(Some(Literal::Int64(v.parse::<i64>()?))),
        DataType::Float32 => {
            let f = v.parse::<f32>()?;
            Ok(Some(Literal::Float32(f)))
        }
        DataType::Float64 => {
            let f = v.parse::<f64>()?;
            Ok(Some(Literal::Float64(f)))
        }
        _ => Ok(None),
    }
}

/// determin if a str looks like float
fn looks_like_float(v: &str) -> bool {
    v.contains('.')
        && v.trim_end_matches('0')
            .split_once('.')
            .filter(|(_, decimal)| !decimal.is_empty())
            .is_some()
}
