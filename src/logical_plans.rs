use crate::expressions::Expression;
use crate::tables::RelationSchema;
use crate::{DBError, DBResult};

#[derive(Debug)]
pub enum LogicalPlan {
    UnResolvedScan {
        table: String,
    },
    Scan {
        table: String,
        schema: RelationSchema,
    },
    Filter {
        expression: Expression,
        child: Box<LogicalPlan>,
    },
}
impl LogicalPlan {
    pub fn schema(&self) -> DBResult<RelationSchema> {
        match self {
            LogicalPlan::UnResolvedScan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
            LogicalPlan::Scan { table: _, schema } => Ok(schema.clone()),
            LogicalPlan::Filter {
                expression: _,
                child,
            } => child.schema(),
        }
    }
}
