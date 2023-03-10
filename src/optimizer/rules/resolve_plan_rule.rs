use std::sync::RwLock;

use crate::{logical_plans::LogicalPlan, optimizer::OptimizerContext, DBError, DBResult};

use super::Rule;

/// Resolve UnResolvedScan node.
pub struct ResolvePlanRule {}

impl Rule<LogicalPlan> for ResolvePlanRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        node.transform_bottom_up(context, Self::resolve_logical_plan)
    }
}

impl ResolvePlanRule {
    fn resolve_logical_plan(
        logical_plan: &LogicalPlan,
        context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        match logical_plan {
            LogicalPlan::UnResolvedScan { table } => {
                context.try_get_table(table.as_str()).and_then(|tbl| {
                    RwLock::read(&tbl)
                        .map_err(|_e| {
                            DBError::Unknown("Access tabl read lock failed.".to_string())
                        })
                        .map(|tbl| {
                            Some(LogicalPlan::Scan {
                                table: table.to_string(),
                                schema: tbl.get_table_meta().get_schema().clone(),
                            })
                        })
                })
            }
            _ => Ok(None),
        }
    }
}
