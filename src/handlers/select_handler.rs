use std::sync::{Arc, RwLock};

use sqlparser::ast::Statement;

use crate::{
    logical_plans::LogicalPlan,
    optimizer::Optimizer,
    parser::build_logical_plan,
    physical_plans::{Filter, Limit, PhysicalPlan, Sort},
    physical_plans::{HashAggregator, Projection},
    Catalog, DBError, DBResult, ResultSet,
};

use super::QueryHandler;

pub struct SelectHandler {
    catalog: Arc<RwLock<Catalog>>,
}

impl QueryHandler for SelectHandler {
    fn handle(&self, statement: Statement) -> DBResult<ResultSet> {
        match statement {
            Statement::Query(query) => self.process_query(*query),
            _ => Err(DBError::Unknown("Should never happen!".to_string())),
        }
    }
}

impl SelectHandler {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    fn process_query(&self, query: sqlparser::ast::Query) -> DBResult<ResultSet> {
        // generate logical plan
        let logical_plan = build_logical_plan(query)?;
        println!("logical plan: {logical_plan:?}");

        // TODO: optimize logical plan before further planning
        let optimizer = Optimizer::new(Arc::clone(&self.catalog));
        let optimized_logical_plan = optimizer.optimize(logical_plan)?;

        println!("optimized logical plan: {optimized_logical_plan:?}");

        // transform to physical plan by planning it
        let mut physical_plan = self.planning(optimized_logical_plan)?;

        // println!("physical plan: {:?}", physical_plan);

        // execute query
        physical_plan.setup()?;
        let mut rs = ResultSet::new(physical_plan.schema()?, Vec::new());
        while let Some(r) = physical_plan.next()? {
            rs.rows.push(r);
        }

        Ok(rs)
    }

    fn planning(&self, logical_plan: LogicalPlan) -> DBResult<Box<dyn PhysicalPlan>> {
        match logical_plan {
            LogicalPlan::Filter { expression, child } => {
                let child_plan = self.planning(*child)?;
                Ok(Box::new(Filter::new(expression, child_plan)))
            }
            LogicalPlan::Scan { table, .. } => {
                let table = RwLock::read(&self.catalog)
                    .map_err(|_e| {
                        DBError::Unknown("access catalog read lock failed".to_string())
                    })?
                    .try_get_table(&table)?;
                let table = RwLock::read(&table).map_err(|_e| {
                    DBError::Unknown("access table read lock failed".to_string())
                })?;

                Ok(table.as_ref().create_scan_op())
            }
            LogicalPlan::UnResolvedScan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
            LogicalPlan::Projection { expressions, child } => {
                let child_plan = self.planning(*child)?;
                Ok(Box::new(Projection::new(expressions, child_plan)))
            }
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                child,
            } => {
                let child_plan = self.planning(*child)?;
                Ok(Box::new(HashAggregator::new(
                    aggregators,
                    groupings,
                    child_plan,
                )))
            }
            LogicalPlan::Sort { options, child } => {
                let child_plan = self.planning(*child)?;
                Ok(Box::new(Sort::new(options, child_plan)))
            }
            LogicalPlan::Limit {
                offset,
                limit,
                child,
            } => Ok(Box::new(Limit::new(offset, limit, self.planning(*child)?))),
            LogicalPlan::UnResolvedHaving { .. } => todo!(),
        }
    }
}
