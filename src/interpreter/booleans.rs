use std::cmp::Ordering;

use crate::{
    expressions::{BinaryOp, Literal, UnaryOp},
    DBError::InterpretingError,
    DBResult,
};

pub fn gt_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    cmp_impl(&left, &right).map(|o| Literal::Bool(o.is_gt()))
}

pub fn gte_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    cmp_impl(&left, &right).map(|o| Literal::Bool(o.is_ge()))
}

pub fn eq_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    cmp_impl(&left, &right).map(|o| Literal::Bool(o.is_eq()))
}

pub fn lt_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    cmp_impl(&left, &right).map(|o| Literal::Bool(o.is_lt()))
}

pub fn lte_impl(left: Literal, right: Literal) -> DBResult<Literal> {
    cmp_impl(&left, &right).map(|o| Literal::Bool(o.is_le()))
}

pub fn cmp_impl(left: &Literal, right: &Literal) -> DBResult<Ordering> {
    match (left, right) {
        (Literal::Int8(l), Literal::Int8(r)) => Ok(l.cmp(r)),
        (Literal::Int16(l), Literal::Int16(r)) => Ok(l.cmp(r)),
        (Literal::Int32(l), Literal::Int32(r)) => Ok(l.cmp(r)),
        (Literal::Int64(l), Literal::Int64(r)) => Ok(l.cmp(r)),
        (Literal::UInt8(l), Literal::UInt8(r)) => Ok(l.cmp(r)),
        (Literal::UInt16(l), Literal::UInt16(r)) => Ok(l.cmp(r)),
        (Literal::UInt32(l), Literal::UInt32(r)) => Ok(l.cmp(r)),
        (Literal::UInt64(l), Literal::UInt64(r)) => Ok(l.cmp(r)),
        // TODO: take care of NaN
        (Literal::Float32(l), Literal::Float32(r)) => Ok(l.partial_cmp(r).unwrap()),
        (Literal::Float64(l), Literal::Float64(r)) => Ok(l.partial_cmp(r).unwrap()),
        (Literal::String(l), Literal::String(r)) => Ok(l.cmp(r)),
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
