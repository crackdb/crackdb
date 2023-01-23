use crate::data_types::DataType;
use crate::DBError;
use crate::{row::Row, DBResult};

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    UnResolvedFieldRef(String),
    FieldRef(usize, DataType),
    BooleanExpr(Box<BooleanExpr>),
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
                    (Literal::Int(left), Literal::Int(right)) => {
                        Ok(Literal::Bool(left > right))
                    }
                    _ => todo!(),
                }
            }
            BooleanExpr::GTE { left: _, right: _ } => todo!(),
            BooleanExpr::EQ { left, right } => {
                match (left.eval(row)?, right.eval(row)?) {
                    (Literal::Int(v1), Literal::Int(v2)) => Ok(Literal::Bool(v1 == v2)),
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
    Int(i32),
    Bool(bool),
    String(String),
    Null,
}

impl Literal {
    pub fn as_bool(&self) -> DBResult<bool> {
        match self {
            Literal::Bool(v) => Ok(*v),
            _ => Err(DBError::Unknown("Cannot convert int to bool.".to_string())),
        }
    }
}
