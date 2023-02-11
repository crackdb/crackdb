use crate::{logical_plans::LogicalPlan, optimizer::OptimizerNode, DBResult};
mod push_down_aggregators_rule;
mod resolve_expr_rule;
mod resolve_functions_rule;
mod resolve_literal_types_rule;
mod resolve_plan_rule;

pub use crate::optimizer::rules::resolve_expr_rule::ResolveExprRule;
use resolve_plan_rule::ResolvePlanRule;

use self::{
    resolve_functions_rule::ResolveFunctionsRule,
    resolve_literal_types_rule::ResolveLiteralTypesRule,
};

use push_down_aggregators_rule::PushDownAggregatorsRule;

/// Optimizer works by applying various rules on tree/graph and transforming the target.
/// Rule is a interface for all rules.
pub trait Rule<T: OptimizerNode> {
    fn apply(&self, node: &T, context: &T::Context) -> DBResult<Option<T>>;
}

pub(crate) fn get_all_rules() -> Vec<Vec<Box<dyn Rule<LogicalPlan>>>> {
    vec![
        vec![
            Box::new(ResolvePlanRule {}),
            Box::new(PushDownAggregatorsRule {}),
        ],
        vec![
            Box::new(ResolveExprRule {}),
            Box::new(ResolveLiteralTypesRule {}),
            Box::new(ResolveFunctionsRule {}),
        ],
    ]
}
