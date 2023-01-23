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
        Cell::UInt8(_v) => todo!(),
        Cell::UInt16(_v) => todo!(),
        Cell::UInt32(_v) => todo!(),
        Cell::UInt64(_v) => todo!(),
        Cell::Int8(_v) => todo!(),
        Cell::Int16(_v) => todo!(),
        Cell::Int32(v) => Literal::Int(*v),
        Cell::Int64(_v) => todo!(),
        Cell::Float32(_v) => todo!(),
        Cell::Float64(_v) => todo!(),
        Cell::String(_v) => todo!(),
        Cell::Boolean(_v) => todo!(),
        Cell::Null => todo!(),
        Cell::DateTime(_v) => todo!(),
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
