use std::cmp::Ordering;

use crate::{
    interpreter::{booleans::cmp_impl, Interpreter},
    logical_plans::SortOption,
    row::Row,
    DBError, DBResult,
};

use super::PhysicalPlan;

pub struct Sort {
    sort_options: Vec<SortOption>,
    child: Box<dyn PhysicalPlan>,
    acc_buffer: Option<Vec<Row<'static>>>,
    output_buffer: Option<Box<dyn Iterator<Item = Row<'static>>>>,
}

impl Sort {
    pub fn new(sort_options: Vec<SortOption>, child: Box<dyn PhysicalPlan>) -> Self {
        Self {
            sort_options,
            child,
            acc_buffer: Some(vec![]),
            output_buffer: None,
        }
    }

    fn try_pull(&mut self) -> DBResult<()> {
        if let Some(buffer) = &mut self.acc_buffer {
            // drain child outputs
            while let Some(row) = self.child.next()? {
                buffer.push(row);
            }

            // sort
            buffer.sort_by(|left, right| {
                self.sort_options
                    .iter()
                    .map(|option| {
                        // TODO: handle errors
                        Self::sort(option, left, right).unwrap()
                    })
                    .find(|o| o.is_ne())
                    .unwrap_or(Ordering::Equal)
            });

            // move on to push stage
            self.output_buffer = self.acc_buffer.take().map(|buffer| {
                Box::new(buffer.into_iter()) as Box<dyn Iterator<Item = Row<'static>>>
            });
        }
        Ok(())
    }

    fn sort(option: &SortOption, left: &Row, right: &Row) -> DBResult<Ordering> {
        let lval = Interpreter::eval(option.expr(), left)?;
        let rval = Interpreter::eval(option.expr(), right)?;
        if option.asc() {
            cmp_impl(&lval, &rval)
        } else {
            cmp_impl(&rval, &lval)
        }
    }

    fn try_push(&mut self) -> DBResult<Option<Row<'static>>> {
        if let Some(iter) = &mut self.output_buffer {
            Ok(iter.next())
        } else {
            Err(DBError::Unknown("should never happen".to_string()))
        }
    }
}

impl PhysicalPlan for Sort {
    fn setup(&mut self) -> crate::DBResult<()> {
        self.child.setup()
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        self.try_pull()?;
        self.try_push()
    }

    fn schema(&self) -> crate::DBResult<crate::tables::RelationSchema> {
        self.child.schema()
    }
}
