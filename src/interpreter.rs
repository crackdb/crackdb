mod arithmetic;
mod booleans;

use crate::{
    expressions::{BinaryOp, Expression, Literal, UnaryOp},
    row::Row,
    DBError, DBResult,
};

use self::{
    arithmetic::{
        divide_impl, max_impl, min_impl, minus_impl, multiply_impl, negative_impl,
        plus_impl,
    },
    booleans::{
        and_impl, eq_impl, gt_impl, gte_impl, lt_impl, lte_impl, not_impl, or_impl,
    },
};

pub struct Interpreter {}

impl Interpreter {
    pub fn eval(expr: &Expression, row: &Row) -> DBResult<Literal> {
        match expr {
            Expression::Literal(l) => Ok(l.clone()),
            Expression::UnResolvedFieldRef(_field) => Err(DBError::Unknown(
                "Trying evaluate an unresolved expression.".to_string(),
            )),
            Expression::FieldRef { index, .. } => row.get_field(*index),
            Expression::BinaryOp { op, left, right } => {
                let (left, right) = (Self::eval(left, row)?, Self::eval(right, row)?);
                match op {
                    BinaryOp::Plus => plus_impl(left, right),
                    BinaryOp::Minus => minus_impl(left, right),
                    BinaryOp::Divide => divide_impl(left, right),
                    BinaryOp::Multiply => multiply_impl(left, right),
                    BinaryOp::Gt => gt_impl(left, right),
                    BinaryOp::Gte => gte_impl(left, right),
                    BinaryOp::Eq => eq_impl(left, right),
                    BinaryOp::Lt => lt_impl(left, right),
                    BinaryOp::Lte => lte_impl(left, right),
                    BinaryOp::And => and_impl(left, right),
                    BinaryOp::Or => or_impl(left, right),
                    BinaryOp::Max => max_impl(left, right),
                    BinaryOp::Min => min_impl(left, right),
                }
            }
            Expression::UnaryOp { op, input } => match op {
                UnaryOp::Not => not_impl(Self::eval(input, row)?),
                UnaryOp::Neg => negative_impl(Self::eval(input, row)?),
            },
            Expression::Alias { alias: _, child } => Self::eval(child, row),
            Expression::UnResolvedFunction { .. } => Err(DBError::InterpretingError(
                "Trying evaluate an unresolved function".to_string(),
            )),
            Expression::Function(_) => todo!(),
            Expression::Wildcard => todo!(),
        }
    }
}
