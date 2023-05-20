use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArgExpr, OrderByExpr, SelectItem, SetExpr,
    TableFactor, UnaryOperator, Value,
};

use crate::{
    expressions::{BinaryOp, Expression, Literal, UnaryOp},
    logical_plans::{LimitOption, LogicalPlan, SortOption},
    DBError, DBResult,
};

fn remove_quotes(value: String) -> String {
    if is_quoted_str(value.as_str()) {
        let bytes = &value.as_bytes()[1..value.len() - 1];
        String::from_utf8_lossy(bytes).to_string()
    } else {
        value
    }
}

fn is_quoted_str(value: &str) -> bool {
    value.len() > 1
        && (value.starts_with('\'') && value.ends_with('\'')
            || value.starts_with('"') && value.ends_with('"'))
}

pub(crate) fn build_logical_plan(query: sqlparser::ast::Query) -> DBResult<LogicalPlan> {
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
                    table: remove_quotes(name.to_string()),
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

            // create Filter node for HAVING clause
            if let Some(having) = select.having {
                plan = LogicalPlan::UnResolvedHaving {
                    prediction: ast_expr_to_plan_expr(&having)?,
                    child: Box::new(plan),
                };
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

    if let Some(limit) = &query.limit {
        let limit_option = match ast_expr_to_plan_expr(limit)? {
            Expression::Literal(Literal::UnResolvedNumber(v)) => {
                Ok(LimitOption::Num(v.parse::<usize>()?))
            }
            _ => Err(DBError::ParserError(format!(
                "unrecognized limit option: {limit:?}"
            ))),
        }?;

        let mut offset = 0;
        if let Some(ast_offset) = &query.offset {
            offset = match ast_expr_to_plan_expr(&ast_offset.value)? {
                Expression::Literal(Literal::UnResolvedNumber(v)) => {
                    Ok(v.parse::<usize>()?)
                }
                _ => Err(DBError::ParserError(format!(
                    "unrecognized offset: {offset:?}"
                ))),
            }?;
        }
        logical_plan = LogicalPlan::Limit {
            offset,
            limit: limit_option,
            child: Box::new(logical_plan),
        };
    }

    Ok(logical_plan)
}

pub(crate) fn is_projection_empty(projections: &Vec<SelectItem>) -> bool {
    projections.is_empty()
        || projections.len() == 1
            && projections
                .iter()
                .next()
                .filter(|p| matches!(p, SelectItem::Wildcard(_)))
                .is_some()
}

pub(crate) fn ast_projection_to_plan_expr(
    projection: SelectItem,
) -> DBResult<Expression> {
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

pub(crate) fn ast_binary_op_to_plan_binary_op(op: &BinaryOperator) -> DBResult<BinaryOp> {
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
        _ => todo!(),
    }
}

pub(crate) fn ast_unary_op_to_plan_unary_op(op: &UnaryOperator) -> DBResult<UnaryOp> {
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

pub(crate) fn ast_expr_to_plan_expr(expr: &Expr) -> DBResult<Expression> {
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
                Value::DoubleQuotedString(v) => Literal::UnResolvedString(v.to_string()),
                Value::Boolean(v) => Literal::Bool(*v),
                Value::Null => Literal::Null,
                _ => todo!(),
            };
            Ok(Expression::Literal(literal))
        }
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
        _ => todo!(),
    }
}

pub(crate) fn ast_function_arg_expr_to_plan_expr(
    arg: &FunctionArgExpr,
) -> DBResult<Expression> {
    match arg {
        FunctionArgExpr::Expr(expr) => ast_expr_to_plan_expr(expr),
        FunctionArgExpr::QualifiedWildcard(_) => todo!(),
        FunctionArgExpr::Wildcard => Ok(Expression::Wildcard),
    }
}
