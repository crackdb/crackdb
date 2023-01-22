mod errors;
pub mod expressions;
pub mod logical_plans;
pub mod pysical_plans;
pub mod tables;
pub use errors::*;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    logical_plans::LogicalPlan,
    tables::{InMemTable, Row},
};

use crate::expressions::{BooleanExpr, Expression, Literal};
use pysical_plans::{Filter, InMemTableScan, PhysicalPlan};
use sqlparser::{
    ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor, Value, Values},
    dialect::GenericDialect,
    parser::Parser,
};

#[derive(Default)]
pub struct CrackDB {
    tables: Arc<RwLock<HashMap<String, Arc<RwLock<InMemTable>>>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResultSet {
    pub headers: Vec<String>,
    // TODO: only i32 colums are supported now
    pub rows: Vec<Vec<i32>>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            headers: vec![],
            rows: vec![],
        }
    }
    pub fn new(headers: Vec<String>, rows: Vec<Vec<i32>>) -> Self {
        ResultSet { headers, rows }
    }
}

impl CrackDB {
    pub fn execute(&self, query: &str) -> Result<ResultSet, DBError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, query)?;
        if statements.len() != 1 {
            return Err(DBError::ParserError(
                "only single query statement is supported.".to_string(),
            ));
        }
        let statement = Iterator::next(&mut statements.into_iter()).unwrap();
        // TODO: log the AST in debug level
        // println!("AST: {:?}", statement);
        // TODO: warn any present but unused nodes in AST
        // TODO: check and validate AST
        match statement {
            Statement::CreateTable {
                or_replace: _,
                temporary: _,
                external: _,
                global: _,
                if_not_exists: _,
                name,
                columns,
                constraints: _,
                hive_distribution: _,
                hive_formats: _,
                table_properties: _,
                with_options: _,
                file_format: _,
                location: _,
                query: _,
                without_rowid: _,
                like: _,
                clone: _,
                engine: _,
                default_charset: _,
                collation: _,
                on_commit: _,
                on_cluster: _,
            } => {
                self.create_table(name, columns)?;
                Ok(ResultSet::empty())
            }
            Statement::Insert {
                or: _,
                into: _,
                table_name,
                columns,
                overwrite: _,
                source,
                partitioned: _,
                after_columns: _,
                table: _,
                on: _,
                returning: _,
            } => {
                self.process_insert(table_name, columns, *source)?;
                Ok(ResultSet::empty())
            }
            Statement::Query(query) => self.process_query(*query),
            _ => Err(DBError::Unknown("statement not supported.".to_string())),
        }
    }

    pub fn new() -> Self {
        CrackDB {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// create a table in CrackDB
    /// simply create a InMemTable and register it into a inmem map keyed by the table name
    fn create_table(
        &self,
        name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::ColumnDef>,
    ) -> Result<(), DBError> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        tables_map.insert(
            name.to_string(),
            Arc::new(RwLock::new(InMemTable::new(columns))),
        );
        Ok(())
    }

    fn process_insert(
        &self,
        table_name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::Ident>,
        source: sqlparser::ast::Query,
    ) -> Result<(), DBError> {
        assert!(
            columns.is_empty(),
            "insert into with columns not supported by now!"
        );

        // FIXME: assuming all inser data are i32 without consulting the table schema
        let rows_to_insert: Vec<Row> = match *source.body {
            SetExpr::Values(Values {
                rows,
                explicit_row: _,
            }) => rows
                .into_iter()
                .map(|r| {
                    let fields: Vec<i32> = r
                        .into_iter()
                        .map(|field| match field {
                            Expr::Value(Value::Number(v, _)) => v.parse::<i32>().unwrap(),
                            Expr::Value(_) => panic!("unsupported insert value type!"),
                            _ => panic!("unsupported insert value!"),
                        })
                        .collect();
                    Row::new(fields)
                })
                .collect(),
            _ => panic!("unsupported insert statement type!"),
        };

        let table = self.try_get_table(table_name.to_string().as_str())?;
        let mut table = RwLock::write(Arc::as_ref(&table)).map_err(|_| {
            DBError::Unknown("Access write lock of table failed!".to_string())
        })?;
        table.insert_data(rows_to_insert);
        Ok(())
    }

    fn process_query(&self, query: sqlparser::ast::Query) -> Result<ResultSet, DBError> {
        // generate logical plan
        let logical_plan = build_logical_plan(query)?;
        // print!("logical plan: {:?}", logical_plan);

        // TODO: optimize logical plan before further planning
        let resolved_logical_plan = self.resolve_logical_plan(logical_plan)?;

        // transform to physical plan by planning it
        let mut pysical_plan = self.planning(resolved_logical_plan)?;

        // execute query
        pysical_plan.setup()?;
        let mut rs = ResultSet::new(pysical_plan.schema()?, Vec::new());
        while let Some(r) = pysical_plan.next()? {
            rs.rows.push(r.fields);
        }

        Ok(rs)
    }

    fn planning(&self, logical_plan: LogicalPlan) -> DBResult<Box<dyn PhysicalPlan>> {
        match logical_plan {
            LogicalPlan::Filter { expression, child } => {
                let child_plan = self.planning(*child)?;
                Ok(Box::new(Filter::new(expression, child_plan)))
            }
            LogicalPlan::ResolvedScan { table, .. } => {
                self.try_get_table(&table).map(|table| {
                    Box::new(InMemTableScan::new(table)) as Box<dyn PhysicalPlan>
                })
            }
            LogicalPlan::Scan { table: _ } => {
                Err(DBError::Unknown("Scan is not resolved.".to_string()))
            }
        }
    }

    fn try_get_table(&self, table_name: &str) -> DBResult<Arc<RwLock<InMemTable>>> {
        self.tables
            .read()
            .map_err(|_| {
                DBError::Unknown("Acceess read lock of tables failed!".to_string())
            })
            .and_then(|tables| {
                tables
                    .get(table_name)
                    .cloned()
                    .ok_or_else(|| DBError::TableNotFound(table_name.to_string()))
            })
    }

    fn resolve_logical_plan(&self, logical_plan: LogicalPlan) -> DBResult<LogicalPlan> {
        match logical_plan {
            LogicalPlan::Scan { table } => {
                self.try_get_table(table.as_str()).and_then(|tbl| {
                    RwLock::read(&tbl)
                        .map_err(|_e| {
                            DBError::Unknown("Access tabl read lock failed.".to_string())
                        })
                        .map(|tbl| LogicalPlan::ResolvedScan {
                            table,
                            columns: tbl.headers(),
                        })
                })
            }
            LogicalPlan::Filter { expression, child } => self
                .resolve_logical_plan(*child)
                .and_then(|resolved_child| {
                    self.resolve_expression(expression, resolved_child.schema()?)
                        .map(|resolved_expr| (resolved_expr, resolved_child))
                })
                .map(|(resolved_expr, resolved_child)| LogicalPlan::Filter {
                    expression: resolved_expr,
                    child: Box::new(resolved_child),
                }),
            LogicalPlan::ResolvedScan { .. } => Ok(logical_plan),
        }
    }

    fn resolve_expression(
        &self,
        expr: Expression,
        schema: Vec<String>,
    ) -> DBResult<Expression> {
        match expr {
            Expression::Literal(_) => Ok(expr),
            Expression::FieldRef(name) => match schema.iter().position(|n| n.eq(&name)) {
                Some(idx) => Ok(Expression::ResolvedFieldRef(idx)),
                None => Err(DBError::Unknown(format!("Cannot find field {}", name))),
            },
            Expression::ResolvedFieldRef(_) => Ok(expr),
            Expression::BooleanExpr(boolean_expr) => self
                .resolve_boolean_expression(*boolean_expr, schema)
                .map(|resolved_expr| Expression::BooleanExpr(Box::new(resolved_expr))),
        }
    }

    fn resolve_boolean_expression(
        &self,
        expr: BooleanExpr,
        schema: Vec<String>,
    ) -> DBResult<BooleanExpr> {
        match expr {
            BooleanExpr::GT { left, right } => self
                .resolve_expression(left, schema.clone())
                .and_then(|resolved_left| {
                    self.resolve_expression(right, schema)
                        .map(|resolved_right| BooleanExpr::GT {
                            left: resolved_left,
                            right: resolved_right,
                        })
                }),
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
                } => LogicalPlan::Scan {
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
        Expr::Identifier(identifier) => {
            Expression::FieldRef(identifier.value.to_string())
        }
        Expr::Value(Value::Number(value, _)) => {
            Expression::Literal(Literal::Int(value.parse::<i32>().unwrap()))
        }
        _ => panic!("not supported yet!"),
    }
}
