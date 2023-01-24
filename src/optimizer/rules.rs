use crate::{logical_plans::LogicalPlan, optimizer::OptimizerNode, DBResult};
mod resolve_expr_rule;
mod resolve_plan_rule;

use crate::optimizer::rules::resolve_expr_rule::ResolveExprRule;
use resolve_plan_rule::ResolvePlanRule;

/// Optimizer works by applying various rules on tree/graph and transforming the target.
/// Rule is a interface for all rules.
pub trait Rule<T: OptimizerNode> {
    fn apply(&self, node: &T, context: &T::Context) -> DBResult<Option<T>>;
}

pub(crate) fn get_all_rules() -> Vec<Box<dyn Rule<LogicalPlan>>> {
    vec![Box::new(ResolvePlanRule {}), Box::new(ResolveExprRule {})]
}
