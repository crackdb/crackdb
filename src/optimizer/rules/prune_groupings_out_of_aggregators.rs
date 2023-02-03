use crate::{
    logical_plans::LogicalPlan,
    optimizer::{OptimizerContext, OptimizerNode},
    DBResult,
};

use super::Rule;

/// This rule filters grouping expressions out of aggregator expressions for
/// LogicalPlan::Aggregator
pub struct PruneGroupingsFromAggregatorsRule {}

impl Rule<LogicalPlan> for PruneGroupingsFromAggregatorsRule {
    fn apply(
        &self,
        node: &LogicalPlan,
        context: &<LogicalPlan as OptimizerNode>::Context,
    ) -> DBResult<Option<LogicalPlan>> {
        node.transform_bottom_up(context, Self::prune_groupings_from_aggregators)
    }
}

impl PruneGroupingsFromAggregatorsRule {
    fn prune_groupings_from_aggregators(
        node: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> DBResult<Option<LogicalPlan>> {
        match node {
            LogicalPlan::Aggregator {
                aggregators,
                groupings,
                child,
            } => {
                let new_aggregators = aggregators
                    .iter()
                    .filter(|agg| agg.is_aggregation())
                    .collect::<Vec<_>>();
                if new_aggregators.len() != aggregators.len() {
                    for _possible_grouping in
                        aggregators.iter().filter(|agg| !agg.is_aggregation())
                    {
                        // TODO: validate and prune groupings in aggregators
                    }
                    Ok(Some(LogicalPlan::Aggregator {
                        aggregators: new_aggregators
                            .iter()
                            .map(|agg| (*agg).clone())
                            .collect(),
                        groupings: groupings.clone(),
                        child: child.clone(),
                    }))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
