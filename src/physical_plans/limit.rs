use crate::{logical_plans::LimitOption, row::Row, tables::RelationSchema, DBResult};

use super::PhysicalPlan;

pub struct Limit {
    offset: usize,
    limit: LimitOption,
    child: Box<dyn PhysicalPlan>,
    pos: usize,
}

impl Limit {
    fn has_next(&self) -> bool {
        let within_limit = match self.limit {
            LimitOption::Num(v) => self.pos < self.offset + v,
            LimitOption::All => true,
        };
        self.pos >= self.offset && within_limit
    }

    pub(crate) fn new(
        offset: usize,
        limit: LimitOption,
        child: Box<dyn PhysicalPlan>,
    ) -> Self {
        Self {
            offset,
            limit,
            child,
            pos: 0,
        }
    }
}

impl PhysicalPlan for Limit {
    fn setup(&mut self) -> crate::DBResult<()> {
        self.child.setup()
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        while let Some(row) = self.child.next()? {
            if self.pos < self.offset {
                self.pos += 1;
            } else if self.has_next() {
                self.pos += 1;
                return Ok(Some(row));
            } else {
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        self.child.schema()
    }
}
