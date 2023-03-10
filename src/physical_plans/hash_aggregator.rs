use std::collections::HashMap;

use crate::{
    aggregators::{aggregator_schema, Aggregator},
    expressions::{Expression, Literal},
    interpreter::Interpreter,
    row::Row,
    tables::RelationSchema,
    DBError, DBResult,
};

use super::PhysicalPlan;

/// first part is grouping values, later part is aggregation results
type AggregatorResult = (Vec<Literal>, Vec<Row<'static>>);

pub struct HashAggregator {
    aggregator_exprs: Vec<Expression>,
    grouping_exprs: Vec<Expression>,
    child: Box<dyn PhysicalPlan>,
    aggregators: Vec<Box<dyn Aggregator>>,
    buffers: Option<HashMap<Vec<Literal>, Vec<Row<'static>>>>,
    iter: Option<Box<dyn Iterator<Item = AggregatorResult>>>,
}

impl HashAggregator {
    pub fn new(
        aggregator_exprs: Vec<Expression>,
        grouping_exprs: Vec<Expression>,
        child: Box<dyn PhysicalPlan>,
    ) -> Self {
        Self {
            aggregator_exprs,
            grouping_exprs,
            child,
            aggregators: vec![],
            buffers: Some(HashMap::new()),
            iter: None,
        }
    }
    fn new_aggregators(&self) -> DBResult<Vec<Box<dyn Aggregator>>> {
        self.aggregator_exprs
            .iter()
            .map(|agg_expr| match agg_expr {
                Expression::Function(func) => func.as_ref().aggregator(),
                _ => Err(DBError::Unknown(format!("unsupported agg: {agg_expr}"))),
            })
            .collect::<DBResult<Vec<_>>>()
    }

    fn pull(&mut self) -> DBResult<()> {
        let buffers = self.buffers.as_mut().expect("should never happen");
        // pulling
        while let Some(row) = self.child.next()? {
            let result_key = self
                .grouping_exprs
                .iter()
                .map(|e| Interpreter::eval(e, &row))
                .collect::<DBResult<Vec<_>>>()?;

            match buffers.get_mut(&result_key) {
                Some(result_rows) => {
                    Self::process(&self.aggregators, &row, result_rows)?;
                }
                None => {
                    let mut result_rows = self
                        .aggregators
                        .iter()
                        .map(|agg| agg.initial_row())
                        .collect::<DBResult<Vec<_>>>()?;
                    Self::process(&self.aggregators, &row, &mut result_rows)?;
                    buffers.insert(result_key, result_rows);
                }
            };
        }
        self.iter = self.buffers.take().map(|buffers| {
            Box::new(buffers.into_iter()) as Box<dyn Iterator<Item = AggregatorResult>>
        });
        Ok(())
    }

    fn process(
        aggregators: &[Box<dyn Aggregator>],
        input: &Row,
        result_rows: &mut [Row],
    ) -> DBResult<()> {
        for (aggregator, result_row) in aggregators.iter().zip(result_rows.iter_mut()) {
            aggregator.process(input, result_row)?;
        }
        Ok(())
    }

    fn try_push(&mut self) -> DBResult<Option<Row<'static>>> {
        let opt_row = self.iter.as_mut().expect("should never happen").next().map(
            |(mut groupings, aggregations)| {
                let agg_results = self
                    .aggregators
                    .iter()
                    .zip(aggregations.iter())
                    .map(|(agg, result)| agg.result(result))
                    .collect::<DBResult<Vec<_>>>();
                agg_results.map(|aggs| {
                    groupings.extend(aggs);
                    Row::new(groupings)
                })
            },
        );
        opt_row.map_or(Ok(None), |r| r.map(Some))
    }
}

impl PhysicalPlan for HashAggregator {
    fn setup(&mut self) -> DBResult<()> {
        self.child.setup()?;
        let child_schema = self.child.schema()?;
        let mut aggs = self.new_aggregators()?;
        for agg in aggs.iter_mut() {
            agg.resolve_expr(&child_schema)?;
        }
        self.aggregators = aggs;
        Ok(())
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        if self.iter.is_none() {
            self.pull()?;
        }
        self.try_push()
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        Ok(aggregator_schema(
            &self.grouping_exprs,
            &self.aggregator_exprs,
        ))
    }
}
