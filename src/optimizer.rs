use std::sync::{Arc, RwLock};

use crate::{
    functions::FunctionsRegistry,
    logical_plans::LogicalPlan,
    tables::{RelationSchema, Table},
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
    functions_registry: FunctionsRegistry,
}

pub struct OptimizerContext {
    catalog: Arc<RwLock<Catalog>>,
}

impl OptimizerContext {
    fn try_get_table(&self, table_name: &str) -> DBResult<Arc<RwLock<Box<dyn Table>>>> {
        let catalog = self
            .catalog
            .read()
            .map_err(|_e| DBError::Unknown("Access db read lock failed.".to_string()))?;
        catalog.try_get_table(table_name)
    }
}

impl OptimizerContextForExpr {
    pub fn new(schema: RelationSchema) -> Self {
        // TODO: use a global functions registry
        OptimizerContextForExpr {
            schema,
            functions_registry: FunctionsRegistry::new(),
        }
    }

    pub fn schema(&self) -> &RelationSchema {
        &self.schema
    }

    pub fn functions_registry(&self) -> &FunctionsRegistry {
        &self.functions_registry
    }
}

pub struct Optimizer {
    rules: Vec<Vec<Box<dyn Rule<LogicalPlan>>>>,
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
        for stage_rules in &self.rules {
            node_under_plan =
                Self::optimize_with_stage_rules(node_under_plan, &context, stage_rules)?;
        }

        Ok(node_under_plan)
    }

    fn optimize_with_stage_rules(
        node: LogicalPlan,
        context: &OptimizerContext,
        rules: &[Box<dyn Rule<LogicalPlan>>],
    ) -> DBResult<LogicalPlan> {
        let mut node_under_plan = node;
        let mut changed = true;

        // FIXME: prevent potential infinite looping
        while changed {
            changed = false;
            for rule in rules.iter() {
                if let Some(new_node) = rule.apply(&node_under_plan, context)? {
                    node_under_plan = new_node;
                    changed = true;
                }
            }
        }

        Ok(node_under_plan)
    }
}
