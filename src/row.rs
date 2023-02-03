use crate::expressions::Literal;
use crate::{DBError, DBResult};

#[derive(Debug, Clone, PartialEq)]
pub enum Row<'a> {
    Simple {
        fields: Vec<Literal>,
    },
    Combined {
        left: &'a Row<'a>,
        right: &'a Row<'a>,
    },
}

impl<'a> Row<'a> {
    pub fn new(fields: Vec<Literal>) -> Self {
        Row::Simple { fields }
    }

    pub fn num_fields(&self) -> usize {
        match self {
            Row::Simple { fields } => fields.len(),
            Row::Combined { left, right } => left.num_fields() + right.num_fields(),
        }
    }

    pub fn get_field(&self, index: usize) -> DBResult<Literal> {
        if index >= self.num_fields() {
            Err(DBError::Unknown("index out of bound".to_string()))
        } else {
            match self {
                Self::Simple { fields } => Self::get_field_at(fields, index),
                Self::Combined { left, right } => {
                    if index >= left.num_fields() {
                        right.get_field(index - left.num_fields())
                    } else {
                        left.get_field(index)
                    }
                }
            }
        }
    }

    pub fn get_field_at(fields: &[Literal], index: usize) -> DBResult<Literal> {
        match fields.get(index) {
            None => Err(DBError::Unknown("Value not exists.".to_string())),
            Some(literal) => Ok(literal.clone()),
        }
    }

    pub(crate) fn concat<'b>(left: &'b Row, right: &'b Row) -> Row<'b> {
        Row::Combined { left, right }
    }

    pub(crate) fn update_field(
        &mut self,
        index: usize,
        literal: Literal,
    ) -> DBResult<()> {
        if index >= self.num_fields() {
            Err(DBError::Unknown("index out of bound".to_string()))
        } else {
            match self {
                Self::Simple { fields } => {
                    fields[index] = literal;
                    Ok(())
                }
                Self::Combined { .. } => {
                    // TODO: redesign Combine row
                    todo!()
                }
            }
        }
    }
}
