mod errors;
pub mod expressions;
pub mod logical_plans;
pub mod pysical_plans;
pub mod tables;
pub use errors::*;
use optimizer::Optimizer;
use tables::{FieldInfo, RelationSchema, TableMeta};
pub mod data_types;
pub mod row;
use data_types::DataType;
use row::Cell;
mod optimizer;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{logical_plans::LogicalPlan, row::Row, tables::InMemTable};

use crate::expressions::{BooleanExpr, Expression, Literal};
use pysical_plans::{Filter, InMemTableScan, PhysicalPlan};
use sqlparser::{
    ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor, Value, Values},
    dialect::GenericDialect,
    parser::Parser,
};

#[derive(Default)]
pub struct CrackDB {
    catalog: Arc<RwLock<Catalog>>,
}

#[derive(Default)]
pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, Arc<RwLock<InMemTable>>>>>,
}

#[derive(Debug, PartialEq)]
pub struct ResultSet {
    pub headers: Vec<String>,
    pub rows: Vec<Row>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            headers: vec![],
            rows: vec![],
        }
    }
    pub fn new(headers: Vec<String>, rows: Vec<Row>) -> Self {
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
        let catalog = Arc::new(RwLock::new(Catalog {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }));
        CrackDB { catalog }
    }

    /// create a table in CrackDB
    /// simply create a InMemTable and register it into a inmem map keyed by the table name
    fn create_table(
        &self,
        name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::ColumnDef>,
    ) -> Result<(), DBError> {
        // TODO: validate against unsupported data types
        let fields = columns
            .into_iter()
            .map(|c| FieldInfo::new(c.name.to_string(), DataType::from(c.data_type)))
            .collect();
        let schema = RelationSchema::new(fields);
        let meta = TableMeta::new(schema);
        RwLock::read(&self.catalog)
            .map_err(|_e| {
                DBError::Unknown("access catalog read lock failed.".to_string())
            })?
            .create_table(name.to_string(), meta)
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

        let schema = {
            let table = RwLock::read(&self.catalog)
                .map_err(|_e| {
                    DBError::Unknown("access catalog read lock failed".to_string())
                })?
                .try_get_table(table_name.to_string().as_str())?;
            let table = RwLock::read(&table).map_err(|_| {
                DBError::Unknown("Access read lock of table failed.".to_string())
            })?;
            table.get_table_meta().get_schema().clone()
        };
        let readers: Vec<(usize, DataType)> = if columns.is_empty() {
            schema
                .get_fields()
                .iter()
                .zip(0..schema.get_fields().len())
                .map(|(f, idx)| (idx, f.data_type().clone()))
                .collect()
        } else {
            // TODO: support insert with columns
            todo!();
        };
        let rows_to_insert: Vec<Row> = match *source.body {
            SetExpr::Values(Values {
                rows,
                explicit_row: _,
            }) => {
                let mut rows_to_insert = Vec::with_capacity(rows.len());
                for r in rows {
                    let mut cells = Vec::with_capacity(readers.len());
                    for (idx, data_type) in readers.iter() {
                        match r.get(*idx) {
                            Some(expr) => {
                                let cell =
                                    convert_insert_expr_to_cell_value(expr, data_type)?;
                                cells.push(cell);
                            }
                            None => cells.push(Cell::Null),
                        }
                    }
                    let row = Row::new(cells);
                    rows_to_insert.push(row);
                }
                rows_to_insert
            }
            _ => panic!("unsupported insert statement type!"),
        };

        let table = RwLock::read(&self.catalog)
            .map_err(|_e| {
                DBError::Unknown("access catalog read lock failed.".to_string())
            })?
            .try_get_table(table_name.to_string().as_str())?;
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
        let optimizer = Optimizer::new(Arc::clone(&self.catalog));
        let optimized_logical_plan = optimizer.optimize(logical_plan)?;

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

impl Catalog {
    pub fn try_get_table(&self, table_name: &str) -> DBResult<Arc<RwLock<InMemTable>>> {
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

    pub fn create_table(&self, name: String, meta: TableMeta) -> Result<(), DBError> {
        let mut tables_map = RwLock::write(Arc::as_ref(&self.tables)).map_err(|_| {
            DBError::Unknown("Access write lock of DB tables failed!".to_string())
        })?;
        tables_map.insert(name, Arc::new(RwLock::new(InMemTable::new(meta))));
        Ok(())
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

fn convert_insert_expr_to_cell_value(
    expr: &Expr,
    data_type: &DataType,
) -> DBResult<Cell> {
    match expr {
        Expr::Value(value) => match value {
            Value::Number(v, _) => match data_type {
                DataType::UInt8 => Ok(v.parse::<u8>().map(Cell::UInt8)?),
                DataType::UInt16 => Ok(v.parse::<u16>().map(Cell::UInt16)?),
                DataType::UInt32 => Ok(v.parse::<u32>().map(Cell::UInt32)?),
                DataType::UInt64 => Ok(v.parse::<u64>().map(Cell::UInt64)?),
                DataType::Int8 => Ok(v.parse::<i8>().map(Cell::Int8)?),
                DataType::Int16 => Ok(v.parse::<i16>().map(Cell::Int16)?),
                DataType::Int32 => Ok(v.parse::<i32>().map(Cell::Int32)?),
                DataType::Int64 => Ok(v.parse::<i64>().map(Cell::Int64)?),
                DataType::Float32 => Ok(v.parse::<f32>().map(Cell::Float32)?),
                DataType::Float64 => Ok(v.parse::<f64>().map(Cell::Float64)?),
                _ => Err(DBError::ParserError(
                    "Unsupported number data type.".to_string(),
                )),
            },
            Value::SingleQuotedString(v) => match data_type {
                DataType::String => Ok(Cell::String(v.to_string())),
                // TODO: validate format
                DataType::DateTime => Ok(Cell::DateTime(v.to_string())),
                _ => Err(DBError::ParserError("Unexpected string.".to_string())),
            },
            Value::DollarQuotedString(_) => {
                Err(DBError::ParserError("Unexpected $ string.".to_string()))
            }
            Value::EscapedStringLiteral(_) => Err(DBError::ParserError(
                "Unexpected escapted string".to_string(),
            )),
            Value::NationalStringLiteral(_) => {
                Err(DBError::ParserError("Unexpected value.".to_string()))
            }
            Value::HexStringLiteral(_) => {
                Err(DBError::ParserError("Unexpected hex string.".to_string()))
            }
            Value::DoubleQuotedString(v) => match data_type {
                DataType::String => Ok(Cell::String(v.to_string())),
                _ => Err(DBError::ParserError("Unexpected string.".to_string())),
            },
            Value::Boolean(v) => match data_type {
                DataType::Boolean => Ok(Cell::Boolean(*v)),
                _ => Err(DBError::ParserError("Unexpected boolean.".to_string())),
            },
            Value::Null => Ok(Cell::Null),
            Value::Placeholder(_) => {
                Err(DBError::ParserError("Unexpected placeholder".to_string()))
            }
            Value::UnQuotedString(_) => Err(DBError::ParserError(
                "Unexpected unquoted string.".to_string(),
            )),
        },
        _ => panic!(""),
    }
}
