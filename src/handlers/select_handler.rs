use std::sync::{Arc, RwLock};

use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor, Value};

use crate::{
    expressions::{BooleanExpr, Expression, Literal},
    logical_plans::LogicalPlan,
    optimizer::Optimizer,
    pysical_plans::{Filter, InMemTableScan, PhysicalPlan},
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
        // print!("logical plan: {:?}", logical_plan);

        // TODO: optimize logical plan before further planning
        let optimizer = Optimizer::new(Arc::clone(&self.catalog));
        let optimized_logical_plan = optimizer.optimize(logical_plan)?;

        // println!("optimized logical plan: {:?}", optimized_logical_plan);

        // transform to physical plan by planning it
        let mut pysical_plan = self.planning(optimized_logical_plan)?;

        // execute query
        pysical_plan.setup()?;
        let mut rs = ResultSet::new(pysical_plan.schema()?, Vec::new());
        while let Some(r) = pysical_plan.next()? {
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
            LogicalPlan::Scan { table, .. } => RwLock::read(&self.catalog)
                .map_err(|_e| {
                    DBError::Unknown("access catalog read lock failed".to_string())
                })?
                .try_get_table(&table)
                .map(|table| {
                    Box::new(InMemTableScan::new(table)) as Box<dyn PhysicalPlan>
                }),
            LogicalPlan::UnResolvedScan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
        }
    }
}

fn build_logical_plan(query: sqlparser::ast::Query) -> DBResult<LogicalPlan> {
    let logical_plan = match *query.body {
        SetExpr::Select(box_select) => {
            let select = *box_select;
            // FIXME: support select from multiple tables
            let table_with_join = select.from.first().unwrap();
            let scan = match &table_with_join.relation {
                TableFactor::Table {
                    name,
                    alias: _,
                    args: _,
                    with_hints: _,
                } => LogicalPlan::UnResolvedScan {
                    table: name.to_string(),
                },
                _ => todo!(),
            };
            if let Some(selection) = &select.selection {
                let filter_expr = ast_expr_to_plan_expr(selection);
                LogicalPlan::Filter {
                    expression: filter_expr,
                    child: Box::new(scan),
                }
            } else {
                scan
            }
        }
        _ => {
            return Err(DBError::Unknown(
                "Query statement not supported!".to_string(),
            ));
        }
    };
    Ok(logical_plan)
}

fn ast_expr_to_plan_expr(expr: &Expr) -> Expression {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => Expression::BooleanExpr(Box::new(BooleanExpr::GT {
            left: ast_expr_to_plan_expr(left),
            right: ast_expr_to_plan_expr(right),
        })),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Expression::BooleanExpr(Box::new(BooleanExpr::EQ {
            left: ast_expr_to_plan_expr(left),
            right: ast_expr_to_plan_expr(right),
        })),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => Expression::BooleanExpr(Box::new(BooleanExpr::GTE {
            left: ast_expr_to_plan_expr(left),
            right: ast_expr_to_plan_expr(right),
        })),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Lt,
            right,
        } => Expression::BooleanExpr(Box::new(BooleanExpr::LT {
            left: ast_expr_to_plan_expr(left),
            right: ast_expr_to_plan_expr(right),
        })),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => Expression::BooleanExpr(Box::new(BooleanExpr::LTE {
            left: ast_expr_to_plan_expr(left),
            right: ast_expr_to_plan_expr(right),
        })),
        Expr::BinaryOp {
            left: _,
            op: _,
            right: _,
        } => todo!(),
        Expr::Identifier(identifier) => {
            Expression::UnResolvedFieldRef(identifier.value.to_string())
        }
        Expr::Value(v) => {
            let literal = match v {
                Value::Number(v, _) => Literal::UnResolvedNumber(v.to_string()),
                Value::SingleQuotedString(v) => Literal::UnResolvedString(v.to_string()),
                Value::DollarQuotedString(_) => todo!(),
                Value::EscapedStringLiteral(_) => todo!(),
                Value::NationalStringLiteral(_) => todo!(),
                Value::HexStringLiteral(_) => todo!(),
                Value::DoubleQuotedString(v) => Literal::UnResolvedString(v.to_string()),
                Value::Boolean(v) => Literal::Bool(*v),
                Value::Null => Literal::Null,
                Value::Placeholder(_) => todo!(),
                Value::UnQuotedString(_) => todo!(),
            };
            Expression::Literal(literal)
        }
        Expr::CompoundIdentifier(_) => todo!(),
        Expr::JsonAccess {
            left: _,
            operator: _,
            right: _,
        } => todo!(),
        Expr::CompositeAccess { expr: _, key: _ } => todo!(),
        Expr::IsFalse(_) => todo!(),
        Expr::IsNotFalse(_) => todo!(),
        Expr::IsTrue(_) => todo!(),
        Expr::IsNotTrue(_) => todo!(),
        Expr::IsNull(_) => todo!(),
        Expr::IsNotNull(_) => todo!(),
        Expr::IsUnknown(_) => todo!(),
        Expr::IsNotUnknown(_) => todo!(),
        Expr::IsDistinctFrom(_, _) => todo!(),
        Expr::IsNotDistinctFrom(_, _) => todo!(),
        Expr::InList {
            expr: _,
            list: _,
            negated: _,
        } => todo!(),
        Expr::InSubquery {
            expr: _,
            subquery: _,
            negated: _,
        } => todo!(),
        Expr::InUnnest {
            expr: _,
            array_expr: _,
            negated: _,
        } => todo!(),
        Expr::Between {
            expr: _,
            negated: _,
            low: _,
            high: _,
        } => todo!(),
        Expr::Like {
            negated: _,
            expr: _,
            pattern: _,
            escape_char: _,
        } => todo!(),
        Expr::ILike {
            negated: _,
            expr: _,
            pattern: _,
            escape_char: _,
        } => todo!(),
        Expr::SimilarTo {
            negated: _,
            expr: _,
            pattern: _,
            escape_char: _,
        } => todo!(),
        Expr::AnyOp(_) => todo!(),
        Expr::AllOp(_) => todo!(),
        Expr::UnaryOp { op: _, expr: _ } => todo!(),
        Expr::Cast {
            expr: _,
            data_type: _,
        } => todo!(),
        Expr::TryCast {
            expr: _,
            data_type: _,
        } => todo!(),
        Expr::SafeCast {
            expr: _,
            data_type: _,
        } => todo!(),
        Expr::AtTimeZone {
            timestamp: _,
            time_zone: _,
        } => todo!(),
        Expr::Extract { field: _, expr: _ } => todo!(),
        Expr::Ceil { expr: _, field: _ } => todo!(),
        Expr::Floor { expr: _, field: _ } => todo!(),
        Expr::Position { expr: _, r#in: _ } => todo!(),
        Expr::Substring {
            expr: _,
            substring_from: _,
            substring_for: _,
        } => todo!(),
        Expr::Trim {
            expr: _,
            trim_where: _,
            trim_what: _,
        } => todo!(),
        Expr::Overlay {
            expr: _,
            overlay_what: _,
            overlay_from: _,
            overlay_for: _,
        } => todo!(),
        Expr::Collate {
            expr: _,
            collation: _,
        } => todo!(),
        Expr::Nested(_) => todo!(),
        Expr::TypedString {
            data_type: _,
            value: _,
        } => todo!(),
        Expr::MapAccess { column: _, keys: _ } => todo!(),
        Expr::Function(_) => todo!(),
        Expr::AggregateExpressionWithFilter { expr: _, filter: _ } => todo!(),
        Expr::Case {
            operand: _,
            conditions: _,
            results: _,
            else_result: _,
        } => todo!(),
        Expr::Exists {
            subquery: _,
            negated: _,
        } => todo!(),
        Expr::Subquery(_) => todo!(),
        Expr::ArraySubquery(_) => todo!(),
        Expr::ListAgg(_) => todo!(),
        Expr::ArrayAgg(_) => todo!(),
        Expr::GroupingSets(_) => todo!(),
        Expr::Cube(_) => todo!(),
        Expr::Rollup(_) => todo!(),
        Expr::Tuple(_) => todo!(),
        Expr::ArrayIndex { obj: _, indexes: _ } => todo!(),
        Expr::Array(_) => todo!(),
        Expr::Interval {
            value: _,
            leading_field: _,
            leading_precision: _,
            last_field: _,
            fractional_seconds_precision: _,
        } => todo!(),
        Expr::MatchAgainst {
            columns: _,
            match_value: _,
            opt_search_modifier: _,
        } => todo!(),
    }
}
