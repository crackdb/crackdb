use crate::expressions::Literal;
use crate::{DBError, DBResult};

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub fields: Vec<Cell>,
}

impl Row {
    pub fn new(fields: Vec<Cell>) -> Self {
        Row { fields }
    }

    pub fn get_field(&self, index: usize) -> DBResult<Literal> {
        match self.fields.get(index) {
            None if index >= self.fields.len() => {
                Err(DBError::Unknown("Index out of bound.".to_string()))
            }
            None => Err(DBError::Unknown("Value not exists.".to_string())),
            Some(cell) => Ok(convert_cell_to_expr_literal(cell)),
        }
    }
}

fn convert_cell_to_expr_literal(cell: &Cell) -> Literal {
    match cell {
        Cell::UInt8(v) => Literal::UInt8(*v),
        Cell::UInt16(v) => Literal::UInt16(*v),
        Cell::UInt32(v) => Literal::UInt32(*v),
        Cell::UInt64(v) => Literal::UInt64(*v),
        Cell::Int8(v) => Literal::Int8(*v),
        Cell::Int16(v) => Literal::Int16(*v),
        Cell::Int32(v) => Literal::Int32(*v),
        Cell::Int64(v) => Literal::Int64(*v),
        Cell::Float32(v) => Literal::Float32(*v),
        Cell::Float64(v) => Literal::Float64(*v),
        Cell::String(v) => Literal::String(v.to_string()),
        Cell::Boolean(v) => Literal::Bool(*v),
        Cell::Null => Literal::Null,
        Cell::DateTime(v) => Literal::DateTime(v.to_string()),
    }
}

pub fn convert_expr_literal_to_cell(expr: &Literal) -> Cell {
    match expr {
        Literal::UInt8(v) => Cell::UInt8(*v),
        Literal::UInt16(v) => Cell::UInt16(*v),
        Literal::UInt32(v) => Cell::UInt32(*v),
        Literal::UInt64(v) => Cell::UInt64(*v),
        Literal::Int8(v) => Cell::Int8(*v),
        Literal::Int16(v) => Cell::Int16(*v),
        Literal::Int32(v) => Cell::Int32(*v),
        Literal::Int64(v) => Cell::Int64(*v),
        Literal::Float32(v) => Cell::Float32(*v),
        Literal::Float64(v) => Cell::Float64(*v),
        Literal::Bool(v) => Cell::Boolean(*v),
        Literal::String(v) => Cell::String(v.to_string()),
        Literal::DateTime(v) => Cell::DateTime(v.to_string()),
        _ => todo!(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
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
    String(String),
    Boolean(bool),
    DateTime(String),
    Null,
}
