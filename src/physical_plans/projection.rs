use crate::{
    expressions::Expression,
    interpreter::Interpreter,
    row::{convert_expr_literal_to_cell, Row},
};

use super::PhysicalPlan;

pub struct Projection {
    projections: Vec<Expression>,
    child: Box<dyn PhysicalPlan>,
}

impl Projection {
    pub fn new(projections: Vec<Expression>, child: Box<dyn PhysicalPlan>) -> Self {
        Self { projections, child }
    }
}

impl PhysicalPlan for Projection {
    fn setup(&mut self) -> crate::DBResult<()> {
        self.child.setup()
    }

    fn next(&mut self) -> crate::DBResult<Option<crate::row::Row>> {
        if let Some(row) = self.child.next()? {
            let mut cells = Vec::new();
            for projection in &self.projections {
                let literal = Interpreter::eval(projection, &row)?;
                let cell = convert_expr_literal_to_cell(&literal);
                cells.push(cell);
            }
            let row = Row::new(cells);
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> crate::DBResult<Vec<String>> {
        Ok(self.projections.iter().map(|p| p.to_string()).collect())
    }
}
