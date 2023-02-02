use std::fmt::Display;

use crate::data_types::DataType;
use crate::optimizer::{OptimizerContextForExpr, OptimizerNode};
use crate::DBError;
use crate::DBResult;

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    UnResolvedFieldRef(String),
    FieldRef {
        name: String,
        index: usize,
        data_type: DataType,
    },
    Alias {
        alias: String,
        child: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOp,
        input: Box<Expression>,
    },
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Literal(l) => l.fmt(f),
            Expression::UnResolvedFieldRef(name) => name.fmt(f),
            Expression::FieldRef { name, .. } => name.fmt(f),
            Expression::Alias { alias, child: _ } => alias.fmt(f),
            Expression::BinaryOp { op, left, right } => {
                write!(f, "{left}_{op}_{right}")
            }
            Expression::UnaryOp { op, input } => write!(f, "{op}_{input}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Plus,
    Minus,
    Divide,
    Multiply,
    Gt,
    Gte,
    Eq,
    Lt,
    Lte,
    And,
    Or,
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Plus => "+".fmt(f),
            BinaryOp::Minus => "-".fmt(f),
            BinaryOp::Divide => "/".fmt(f),
            BinaryOp::Multiply => "*".fmt(f),
            BinaryOp::Gt => ">".fmt(f),
            BinaryOp::Gte => ">=".fmt(f),
            BinaryOp::Eq => "=".fmt(f),
            BinaryOp::Lt => "<".fmt(f),
            BinaryOp::Lte => "<=".fmt(f),
            BinaryOp::And => "AND".fmt(f),
            BinaryOp::Or => "OR".fmt(f),
        }
    }
}

impl BinaryOp {
    pub fn is_boolean_op(&self) -> bool {
        matches!(
            self,
            BinaryOp::Gt
                | BinaryOp::Gte
                | BinaryOp::Eq
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::And
                | BinaryOp::Or
        )
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Not,
    Neg,
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOp::Not => "NOT".fmt(f),
            UnaryOp::Neg => "-".fmt(f),
        }
    }
}

impl OptimizerNode for Expression {
    type Context = OptimizerContextForExpr;
}

impl Expression {
    pub fn data_type(&self) -> DataType {
        match self {
            Expression::Literal(l) => l.data_type(),
            Expression::UnResolvedFieldRef(_) => DataType::Unknown,
            Expression::FieldRef { data_type, .. } => data_type.clone(),
            Expression::BinaryOp { op, left, right: _ } => match op {
                op if op.is_boolean_op() => DataType::Boolean,
                _ => left.data_type(),
            },
            Expression::UnaryOp { input, .. } => input.data_type(),
            Expression::Alias { alias: _, child } => child.data_type(),
        }
    }

    pub fn transform_bottom_up(
        &self,
        context: &OptimizerContextForExpr,
        func: fn(&Expression, &OptimizerContextForExpr) -> DBResult<Option<Expression>>,
    ) -> DBResult<Option<Expression>> {
        match self {
            Expression::Literal(_) => func(self, context),
            Expression::UnResolvedFieldRef(_) => func(self, context),
            Expression::FieldRef { .. } => func(self, context),
            Expression::BinaryOp { op, left, right } => {
                match (
                    left.transform_bottom_up(context, func)?,
                    right.transform_bottom_up(context, func)?,
                ) {
                    (None, None) => func(self, context),
                    (None, Some(right)) => {
                        let updated = Expression::BinaryOp {
                            op: op.clone(),
                            left: left.clone(),
                            right: Box::new(right),
                        };
                        match func(&updated, context)? {
                            None => Ok(Some(updated)),
                            Some(updated) => Ok(Some(updated)),
                        }
                    }
                    (Some(left), None) => {
                        let updated = Expression::BinaryOp {
                            op: op.clone(),
                            left: Box::new(left),
                            right: right.clone(),
                        };
                        match func(&updated, context)? {
                            None => Ok(Some(updated)),
                            Some(updated) => Ok(Some(updated)),
                        }
                    }
                    (Some(left), Some(right)) => {
                        let updated = Expression::BinaryOp {
                            op: op.clone(),
                            left: Box::new(left),
                            right: Box::new(right),
                        };
                        match func(&updated, context)? {
                            None => Ok(Some(updated)),
                            Some(updated) => Ok(Some(updated)),
                        }
                    }
                }
            }
            Expression::UnaryOp { op, input } => {
                match input.transform_bottom_up(context, func)? {
                    Some(updated_input) => {
                        let updated = Expression::UnaryOp {
                            op: op.clone(),
                            input: Box::new(updated_input),
                        };
                        match func(&updated, context)? {
                            Some(updated) => Ok(Some(updated)),
                            None => Ok(Some(updated)),
                        }
                    }
                    None => func(self, context),
                }
            }
            Expression::Alias { alias, child } => {
                match child.transform_bottom_up(context, func)? {
                    Some(updated_child) => {
                        let updated = Expression::Alias {
                            alias: alias.clone(),
                            child: Box::new(updated_child),
                        };
                        match func(&updated, context)? {
                            Some(updated) => Ok(Some(updated)),
                            None => Ok(Some(updated)),
                        }
                    }
                    None => func(self, context),
                }
            }
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

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::UnResolvedNumber(n) => n.fmt(f),
            Literal::UnResolvedString(s) => s.fmt(f),
            Literal::UInt8(v) => v.fmt(f),
            Literal::UInt16(v) => v.fmt(f),
            Literal::UInt32(v) => v.fmt(f),
            Literal::UInt64(v) => v.fmt(f),
            Literal::Int8(v) => v.fmt(f),
            Literal::Int16(v) => v.fmt(f),
            Literal::Int32(v) => v.fmt(f),
            Literal::Int64(v) => v.fmt(f),
            Literal::Float32(v) => v.fmt(f),
            Literal::Float64(v) => v.fmt(f),
            Literal::Bool(v) => v.fmt(f),
            Literal::String(v) => v.fmt(f),
            Literal::DateTime(v) => v.fmt(f),
            Literal::Null => "null".fmt(f),
        }
    }
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
