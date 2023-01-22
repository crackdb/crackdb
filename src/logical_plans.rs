use crate::expressions::Expression;
use crate::{DBError, DBResult};

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table: String,
    },
    ResolvedScan {
        table: String,
        columns: Vec<String>,
    },
    Filter {
        expression: Expression,
        child: Box<LogicalPlan>,
    },
}
impl LogicalPlan {
    pub fn schema(&self) -> DBResult<Vec<String>> {
        match self {
            LogicalPlan::Scan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
            LogicalPlan::ResolvedScan { table: _, columns } => Ok(columns.clone()),
            LogicalPlan::Filter {
                expression: _,
                child,
            } => child.schema(),
        }
    }
}
