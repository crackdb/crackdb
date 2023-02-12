use crate::{
    errors::DBResult, expressions::Expression, interpreter::Interpreter, row::Row,
    tables::RelationSchema,
};

use super::PhysicalPlan;

pub struct Filter {
    expression: Expression,
    child: Box<dyn PhysicalPlan>,
}

impl Filter {
    pub fn new(expression: Expression, child: Box<dyn PhysicalPlan>) -> Self {
        Self { expression, child }
    }
}

impl PhysicalPlan for Filter {
    fn setup(&mut self) -> DBResult<()> {
        self.child.setup()
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        while let Some(row) = self.child.next()? {
            if Interpreter::eval(&self.expression, &row)?.as_bool()? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        self.child.schema()
    }
}
