use std::sync::{Arc, RwLock};

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArgExpr, OrderByExpr, SelectItem, SetExpr,
    Statement, TableFactor, UnaryOperator, Value,
};

use crate::{
    expressions::{BinaryOp, Expression, Literal, UnaryOp},
    logical_plans::{LogicalPlan, SortOption},
    optimizer::Optimizer,
    physical_plans::{Filter, InMemTableScan, PhysicalPlan, Sort},
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
        // println!("logical plan: {:?}", logical_plan);

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
        }
    }
}

fn build_logical_plan(query: sqlparser::ast::Query) -> DBResult<LogicalPlan> {
    let mut logical_plan = match *query.body {
        SetExpr::Select(box_select) => {
            let select = *box_select;

            // create Scan node
            // FIXME: support select from multiple tables
            let table_with_join = select.from.first().unwrap();
            let mut plan = match &table_with_join.relation {
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

            // create Filter node
            if let Some(selection) = &select.selection {
                let filter_expr = ast_expr_to_plan_expr(selection)?;
                plan = LogicalPlan::Filter {
                    expression: filter_expr,
                    child: Box::new(plan),
                };
            }

            // create Aggregator node
            if !select.group_by.is_empty() {
                let groupings = select
                    .group_by
                    .iter()
                    .map(ast_expr_to_plan_expr)
                    .collect::<DBResult<Vec<_>>>()?;

                let aggregators = vec![];
                plan = LogicalPlan::Aggregator {
                    aggregators,
                    groupings,
                    child: Box::new(plan),
                };
            }

            // create Projection node
            let projections = select.projection;
            if !is_projection_empty(&projections) {
                let projection_exprs = projections
                    .into_iter()
                    .map(ast_projection_to_plan_expr)
                    .collect::<DBResult<Vec<_>>>()?;

                plan = LogicalPlan::Projection {
                    expressions: projection_exprs,
                    child: Box::new(plan),
                }
            }
            plan
        }
        _ => {
            return Err(DBError::Unknown(
                "Query statement not supported!".to_string(),
            ));
        }
    };

    if !query.order_by.is_empty() {
        let options = query
            .order_by
            .iter()
            .map(|OrderByExpr { expr, asc, .. }| {
                ast_expr_to_plan_expr(expr)
                    .map(|expr| SortOption::new(expr, asc.unwrap_or(true)))
            })
            .collect::<DBResult<Vec<_>>>()?;
        logical_plan = LogicalPlan::Sort {
            options,
            child: Box::new(logical_plan),
        }
    }
    Ok(logical_plan)
}

fn is_projection_empty(projections: &Vec<SelectItem>) -> bool {
    projections.is_empty()
        || projections.len() == 1
            && projections
                .iter()
                .next()
                .filter(|p| matches!(p, SelectItem::Wildcard(_)))
                .is_some()
}

fn ast_projection_to_plan_expr(projection: SelectItem) -> DBResult<Expression> {
    match projection {
        SelectItem::UnnamedExpr(expr) => ast_expr_to_plan_expr(&expr),
        SelectItem::ExprWithAlias { expr, alias } => {
            ast_expr_to_plan_expr(&expr).map(|expr| Expression::Alias {
                alias: alias.to_string(),
                child: Box::new(expr),
            })
        }
        SelectItem::QualifiedWildcard(_, _) => todo!(),
        SelectItem::Wildcard(_) => todo!(),
    }
}

fn ast_binary_op_to_plan_binary_op(op: &BinaryOperator) -> DBResult<BinaryOp> {
    match op {
        BinaryOperator::Plus => Ok(BinaryOp::Plus),
        BinaryOperator::Minus => Ok(BinaryOp::Minus),
        BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
        BinaryOperator::Divide => Ok(BinaryOp::Divide),
        BinaryOperator::Modulo => todo!(),
        BinaryOperator::StringConcat => todo!(),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::GtEq => Ok(BinaryOp::Gte),
        BinaryOperator::LtEq => Ok(BinaryOp::Lte),
        BinaryOperator::Spaceship => todo!(),
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => todo!(),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        BinaryOperator::Xor => todo!(),
        BinaryOperator::BitwiseOr => todo!(),
        BinaryOperator::BitwiseAnd => todo!(),
        BinaryOperator::BitwiseXor => todo!(),
        BinaryOperator::PGBitwiseXor => todo!(),
        BinaryOperator::PGBitwiseShiftLeft => todo!(),
        BinaryOperator::PGBitwiseShiftRight => todo!(),
        BinaryOperator::PGRegexMatch => todo!(),
        BinaryOperator::PGRegexIMatch => todo!(),
        BinaryOperator::PGRegexNotMatch => todo!(),
        BinaryOperator::PGRegexNotIMatch => todo!(),
        BinaryOperator::PGCustomBinaryOperator(_) => todo!(),
    }
}

fn ast_unary_op_to_plan_unary_op(op: &UnaryOperator) -> DBResult<UnaryOp> {
    match op {
        UnaryOperator::Plus => todo!(),
        UnaryOperator::Minus => Ok(UnaryOp::Neg),
        UnaryOperator::Not => Ok(UnaryOp::Not),
        UnaryOperator::PGBitwiseNot => todo!(),
        UnaryOperator::PGSquareRoot => todo!(),
        UnaryOperator::PGCubeRoot => todo!(),
        UnaryOperator::PGPostfixFactorial => todo!(),
        UnaryOperator::PGPrefixFactorial => todo!(),
        UnaryOperator::PGAbs => todo!(),
    }
}

fn ast_expr_to_plan_expr(expr: &Expr) -> DBResult<Expression> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let (left, op, right) = (
                ast_expr_to_plan_expr(left)?,
                ast_binary_op_to_plan_binary_op(op)?,
                ast_expr_to_plan_expr(right)?,
            );
            Ok(Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::Identifier(identifier) => {
            Ok(Expression::UnResolvedFieldRef(identifier.value.to_string()))
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
            Ok(Expression::Literal(literal))
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
        Expr::UnaryOp { op, expr } => {
            let (op, expr) = (
                ast_unary_op_to_plan_unary_op(op)?,
                ast_expr_to_plan_expr(expr)?,
            );
            Ok(Expression::UnaryOp {
                op,
                input: Box::new(expr),
            })
        }
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
        Expr::Function(Function { name, args, .. }) => {
            let arg_exprs = args
                .iter()
                .map(|arg| match arg {
                    sqlparser::ast::FunctionArg::Named { arg, .. } => {
                        ast_function_arg_expr_to_plan_expr(arg)
                    }
                    sqlparser::ast::FunctionArg::Unnamed(arg) => {
                        ast_function_arg_expr_to_plan_expr(arg)
                    }
                })
                .collect::<DBResult<Vec<_>>>()?;
            Ok(Expression::UnResolvedFunction {
                name: name.to_string(),
                args: arg_exprs,
            })
        }
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

fn ast_function_arg_expr_to_plan_expr(arg: &FunctionArgExpr) -> DBResult<Expression> {
    match arg {
        FunctionArgExpr::Expr(expr) => ast_expr_to_plan_expr(expr),
        FunctionArgExpr::QualifiedWildcard(_) => todo!(),
        FunctionArgExpr::Wildcard => Ok(Expression::Wildcard),
    }
}
