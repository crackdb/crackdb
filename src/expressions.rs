use crate::DBError;
use crate::{tables::Row, DBResult};

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    FieldRef(String),
    ResolvedFieldRef(usize),
    BooleanExpr(Box<BooleanExpr>),
}
impl Expression {
    pub fn eval(&self, row: &Row) -> DBResult<Literal> {
        match self {
            Self::Literal(l) => Ok(l.clone()),
            Self::FieldRef(_field) => Err(crate::DBError::Unknown(
                "Trying evaluate an unresolved expression.".to_string(),
            )),
            Self::ResolvedFieldRef(idx) => row.get_field(*idx),
            Self::BooleanExpr(ref bool_expr) => bool_expr.eval(row),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BooleanExpr {
    GT { left: Expression, right: Expression },
}

impl BooleanExpr {
    pub fn eval(&self, row: &Row) -> DBResult<Literal> {
        match self {
            BooleanExpr::GT { left, right } => {
                match (left.eval(row)?, right.eval(row)?) {
                    (Literal::Int(left), Literal::Int(right)) => {
                        Ok(Literal::Bool(left > right))
                    }
                    _ => Err(crate::DBError::Unknown(
                        "Boolean expression evaluation not supported!".to_string(),
                    )),
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int(i32),
    Bool(bool),
}

impl Literal {
    pub fn as_bool(&self) -> DBResult<bool> {
        match self {
            Literal::Int(_) => {
                Err(DBError::Unknown("Cannot convert int to bool.".to_string()))
            }
            Literal::Bool(v) => Ok(*v),
        }
    }
}
