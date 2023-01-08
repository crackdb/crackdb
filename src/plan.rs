use crate::tables::Row;

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table: String,
    },
    Filter {
        expression: Expression,
        child: Box<LogicalPlan>,
    },
}

#[derive(Debug)]
pub enum Expression {
    Literal(Literal),
    FieldRef(String),
    BooleanExpr(Box<BooleanExpr>),
}
impl Expression {
    pub fn eval(&self, _row: &Row) -> bool {
        // TODO: implement this
        true
    }
}

#[derive(Debug)]
pub enum BooleanExpr {
    GT { left: Expression, right: Expression },
}

#[derive(Debug)]
pub enum Literal {
    Int(i64),
}
