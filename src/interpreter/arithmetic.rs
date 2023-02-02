use crate::{
    expressions::{BinaryOp, Literal, UnaryOp},
    DBError::InterpretingError,
    DBResult,
};

pub fn plus_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Int8(l + r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Int16(l + r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Int32(l + r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Int64(l + r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::UInt8(l + r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::UInt16(l + r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::UInt32(l + r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::UInt64(l + r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Float32(l + r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Float64(l + r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Plus,
            left,
            right
        ))),
    }
}

pub fn minus_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Int8(l - r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Int16(l - r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Int32(l - r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Int64(l - r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::UInt8(l - r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::UInt16(l - r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::UInt32(l - r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::UInt64(l - r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Float32(l - r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Float64(l - r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Minus,
            left,
            right
        ))),
    }
}

pub fn divide_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Int8(l / r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Int16(l / r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Int32(l / r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Int64(l / r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::UInt8(l / r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::UInt16(l / r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::UInt32(l / r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::UInt64(l / r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Float32(l / r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Float64(l / r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Divide,
            left,
            right
        ))),
    }
}

pub fn multiply_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(Literal::Int8(l * r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(Literal::Int16(l * r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(Literal::Int32(l * r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(Literal::Int64(l * r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(Literal::UInt8(l * r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(Literal::UInt16(l * r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(Literal::UInt32(l * r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(Literal::UInt64(l * r)),
        (Literal::Float32(l), Literal::Float32(r)) => Ok(Literal::Float32(l * r)),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(Literal::Float64(l * r)),
        (left, right) => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?} and {:?}",
            BinaryOp::Multiply,
            left,
            right
        ))),
    }
}

pub fn negative_impl(input: Literal) -> DBResult<Literal> {
    match input {
        Literal::Int8(v) => Ok(Literal::Int8(-v)),
        Literal::Int16(v) => Ok(Literal::Int16(-v)),
        Literal::Int32(v) => Ok(Literal::Int32(-v)),
        Literal::Int64(v) => Ok(Literal::Int64(-v)),
        Literal::Float32(v) => Ok(Literal::Float32(-v)),
        Literal::Float64(v) => Ok(Literal::Float64(-v)),
        input => Err(InterpretingError(format!(
            "{:?} operator not implemented for {:?}",
            UnaryOp::Neg,
            input
        ))),
    }
}
