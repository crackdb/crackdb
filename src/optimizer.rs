use std::sync::{Arc, RwLock};

use crate::{
    logical_plans::LogicalPlan,
    tables::{InMemTable, RelationSchema},
    Catalog, DBError, DBResult,
};

use self::rules::{get_all_rules, Rule};
pub mod rules;

/// Optimizer works on tree/graph of nodes, e.g. logical plan or expressions.
/// OptimizerNode is the primitive node in the tree/graph.
pub trait OptimizerNode {
    type Context;
}

#[derive(Debug)]
pub struct OptimizerContextForExpr {
    schema: RelationSchema,
}

pub struct OptimizerContext {
    catalog: Arc<RwLock<Catalog>>,
}
impl OptimizerContext {
    fn try_get_table(&self, table_name: &str) -> DBResult<Arc<RwLock<InMemTable>>> {
        let db = self
            .catalog
            .read()
            .map_err(|_e| DBError::Unknown("Access db read lock failed.".to_string()))?;
        db.try_get_table(table_name)
    }
}

impl OptimizerContextForExpr {
    pub fn new(schema: RelationSchema) -> Self {
        OptimizerContextForExpr { schema }
    }

    pub fn schema(&self) -> &RelationSchema {
        &self.schema
    }
}

pub struct Optimizer {
    rules: Vec<Box<dyn Rule<LogicalPlan>>>,
    catalog: Arc<RwLock<Catalog>>,
}

impl Optimizer {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        let rules = get_all_rules();
        Optimizer { rules, catalog }
    }
    pub fn optimize(&self, plan: LogicalPlan) -> DBResult<LogicalPlan> {
        let context = OptimizerContext {
            catalog: Arc::clone(&self.catalog),
        };
        let mut node_under_plan = plan;
        let mut changed = true;

        // FIXME: prevent potential infinite looping
        while changed {
            changed = false;
            for rule in self.rules.iter() {
                if let Some(new_node) = rule.apply(&node_under_plan, &context)? {
                    node_under_plan = new_node;
                    changed = true;
                }
            }
        }
        Ok(node_under_plan)
    }
}
