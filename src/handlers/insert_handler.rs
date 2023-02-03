use std::sync::{Arc, RwLock};

use sqlparser::ast::{Expr, SetExpr, Statement, Value, Values};

use crate::{
    data_types::DataType, expressions::Literal, row::Row, Catalog, DBError, DBResult,
    ResultSet,
};

use super::QueryHandler;

pub struct InsertHandler {
    catalog: Arc<RwLock<Catalog>>,
}

impl QueryHandler for InsertHandler {
    fn handle(
        &self,
        statement: sqlparser::ast::Statement,
    ) -> crate::DBResult<crate::ResultSet> {
        match statement {
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
            _ => Err(DBError::Unknown("should never happen!".to_string())),
        }
    }
}

impl InsertHandler {
    pub(crate) fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
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
                            None => cells.push(Literal::Null),
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
}

fn convert_insert_expr_to_cell_value(
    expr: &Expr,
    data_type: &DataType,
) -> DBResult<Literal> {
    match expr {
        Expr::Value(value) => match value {
            Value::Number(v, _) => match data_type {
                DataType::UInt8 => Ok(v.parse::<u8>().map(Literal::UInt8)?),
                DataType::UInt16 => Ok(v.parse::<u16>().map(Literal::UInt16)?),
                DataType::UInt32 => Ok(v.parse::<u32>().map(Literal::UInt32)?),
                DataType::UInt64 => Ok(v.parse::<u64>().map(Literal::UInt64)?),
                DataType::Int8 => Ok(v.parse::<i8>().map(Literal::Int8)?),
                DataType::Int16 => Ok(v.parse::<i16>().map(Literal::Int16)?),
                DataType::Int32 => Ok(v.parse::<i32>().map(Literal::Int32)?),
                DataType::Int64 => Ok(v.parse::<i64>().map(Literal::Int64)?),
                DataType::Float32 => Ok(v.parse::<f32>().map(Literal::Float32)?),
                DataType::Float64 => Ok(v.parse::<f64>().map(Literal::Float64)?),
                _ => Err(DBError::ParserError(
                    "Unsupported number data type.".to_string(),
                )),
            },
            Value::SingleQuotedString(v) => match data_type {
                DataType::String => Ok(Literal::String(v.to_string())),
                // TODO: validate format
                DataType::DateTime => Ok(Literal::DateTime(v.to_string())),
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
                DataType::String => Ok(Literal::String(v.to_string())),
                _ => Err(DBError::ParserError("Unexpected string.".to_string())),
            },
            Value::Boolean(v) => match data_type {
                DataType::Boolean => Ok(Literal::Bool(*v)),
                _ => Err(DBError::ParserError("Unexpected boolean.".to_string())),
            },
            Value::Null => Ok(Literal::Null),
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
