use crate::{
    expressions::Expression,
    interpreter::Interpreter,
    row::Row,
    tables::{FieldInfo, RelationSchema},
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

    fn next(&mut self) -> crate::DBResult<Option<Row<'static>>> {
        if let Some(row) = self.child.next()? {
            let mut cells = Vec::new();
            for projection in &self.projections {
                let literal = Interpreter::eval(projection, &row)?;
                cells.push(literal);
            }
            let row = Row::new(cells);
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> crate::DBResult<RelationSchema> {
        let fields = self
            .projections
            .iter()
            .map(|f| FieldInfo::new(f.to_string(), f.data_type()))
            .collect();
        let schema = RelationSchema::new(fields);
        Ok(schema)
    }
}
