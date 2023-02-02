use crate::{
    expressions::{BinaryOp, Literal, UnaryOp},
    DBError::InterpretingError,
    DBResult,
};

pub fn gt_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Bool(l > r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Bool(l > r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Bool(l > r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Bool(l > r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::Bool(l > r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::Bool(l > r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::Bool(l > r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::Bool(l > r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Bool(l > r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Bool(l > r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Gt,
            left,
            right
        ))),
    }
}

pub fn gte_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Bool(l >= r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Bool(l >= r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Gte,
            left,
            right
        ))),
    }
}

pub fn eq_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Bool(l == r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Bool(l == r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Bool(l == r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Bool(l == r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::Bool(l == r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::Bool(l == r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::Bool(l == r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::Bool(l == r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Bool(l == r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Bool(l == r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Eq,
            left,
            right
        ))),
    }
}

pub fn lt_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Bool(l < r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Bool(l < r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Bool(l < r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Bool(l < r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::Bool(l < r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::Bool(l < r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::Bool(l < r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::Bool(l < r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Bool(l < r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Bool(l < r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Lt,
            left,
            right
        ))),
    }
}

pub fn lte_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Bool(l <= r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Bool(l <= r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Lte,
            left,
            right
        ))),
    }
}

pub fn and_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Bool(l), Literal::Bool(r)) => Ok(Literal::Bool(l && r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::And,
            left,
            right
        ))),
    }
}

pub fn or_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Bool(l), Literal::Bool(r)) => Ok(Literal::Bool(l || r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Or,
            left,
            right
        ))),
    }
}

pub fn not_impl(input: Literal) -> DBResult<Literal> {
    match input {
        Literal::Bool(v) => Ok(Literal::Bool(!v)),
        input => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?}",
            UnaryOp::Not,
            input
        ))),
    }
}
