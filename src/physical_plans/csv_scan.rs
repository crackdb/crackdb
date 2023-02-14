use crate::{
    data_types::DataType,
    expressions::Literal,
    row::Row,
    tables::{csv::CsvRecordReader, RelationSchema},
    DBResult,
};

use super::PhysicalPlan;

pub struct CsvScan {
    schema: RelationSchema,
    path: String,
    reader: Option<CsvRecordReader>,
}

impl CsvScan {
    pub fn new(path: String, schema: RelationSchema) -> Self {
        Self {
            schema,
            path,
            reader: None,
        }
    }

    fn parse_value(value: String, data_type: &DataType) -> DBResult<Literal> {
        match data_type {
            DataType::Int64 => Ok(Literal::Int64(value.parse::<i64>()?)),
            DataType::Float64 => Ok(Literal::Float64(value.parse::<f64>()?)),
            DataType::String => Ok(Literal::String(value)) as DBResult<Literal>,
            DataType::Boolean => {
                Ok(Literal::Bool(value.as_str().to_lowercase() == "true"))
            }
            _ => unreachable!(),
        }
    }
}

impl PhysicalPlan for CsvScan {
    fn setup(&mut self) -> DBResult<()> {
        let mut reader = CsvRecordReader::new(self.path.as_str())?;
        // skip header
        let _ = reader.try_read_next()?;
        self.reader = Some(reader);
        Ok(())
    }

    fn next(&mut self) -> DBResult<Option<Row<'static>>> {
        if let Some(record) = self.reader.as_mut().unwrap().try_read_next()? {
            if record.len() != self.schema.num_fields() {
                return Err(crate::DBError::StorageEngine(
                    "The csv record is not aligned with schema.".to_owned(),
                ));
            }

            let cells = Iterator::zip(record.into_iter(), self.schema.get_fields())
                .map(|(r, f)| Self::parse_value(r, f.data_type()))
                .collect::<DBResult<Vec<_>>>()?;
            Ok(Some(Row::new(cells)))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> DBResult<RelationSchema> {
        Ok(self.schema.clone())
    }
}
