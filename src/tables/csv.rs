use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use csv_core::{Reader, ReaderBuilder};

use crate::{
    data_types::DataType,
    physical_plans::{CsvScan, PhysicalPlan},
    row::Row,
    DBError, DBResult,
};

use super::{FieldInfo, RelationSchema, Table, TableMeta};

const NUM_RECORDS_TO_INFER_SCHEMA: usize = 10;

/// Note, CsvTable is readonly to a csv file.
/// Only a limit set of data types are supported: String, boolean, Int64, Float64, DateTime.
pub struct CsvTable {
    schema: RelationSchema,
    path: String,
}

impl CsvTable {
    pub fn new(path: String) -> DBResult<Self> {
        let mut reader = CsvRecordReader::new(path.as_str())?;
        // assume first line is headers
        let headers: Vec<String> = reader.try_read_next()?.ok_or(
            DBError::StorageEngine("Provided csv file is empty.".to_owned()),
        )?;

        // read ahead NUM_RECORDS_TO_INFER_SCHEMA lines to determine the field data types
        let mut data_types = vec![DataType::Unknown; headers.len()];
        let mut num_records_read = 0;
        while let Some(result_record) = reader.try_read_next()? {
            num_records_read += 1;
            // TODO: better to provide the line # for debugability
            if result_record.len() != headers.len() {
                return Err(DBError::StorageEngine(
                    "Csv records is not align with headers.".to_owned(),
                ));
            }
            for (idx, field) in result_record.into_iter().enumerate() {
                let data_type = Self::determine_data_type(field, &data_types[idx]);
                data_types[idx] = data_type;
            }
            if num_records_read >= NUM_RECORDS_TO_INFER_SCHEMA {
                break;
            }
        }

        let fields = Iterator::zip(headers.into_iter(), data_types.into_iter())
            .map(|(name, data_type)| FieldInfo::new(name, data_type))
            .collect();
        let schema = RelationSchema::new(fields);
        Ok(CsvTable { schema, path })
    }

    fn determine_data_type(field: String, type_hint: &DataType) -> DataType {
        let lower_case = field.to_lowercase();
        // TODO: support date time
        let data_type = match lower_case.as_str() {
            "true" => DataType::Boolean,
            "false" => DataType::Boolean,
            _ if lower_case.parse::<i64>().is_ok() => DataType::Int64,
            _ if lower_case.parse::<f64>().is_ok() => DataType::Float64,
            _ => DataType::String,
        };
        if &data_type == type_hint {
            data_type
        } else {
            match Self::data_type_generality(&data_type)
                .cmp(&Self::data_type_generality(type_hint))
            {
                std::cmp::Ordering::Less => type_hint.clone(),
                std::cmp::Ordering::Equal => DataType::String,
                std::cmp::Ordering::Greater => data_type,
            }
        }
    }

    /// a data type with higer generality order will win when determin/reconcil two data types
    fn data_type_generality(data_type: &DataType) -> u8 {
        match data_type {
            DataType::Int64 => 1,
            // Float has higher generality than Int
            DataType::Float64 => 2,
            // String has most generality, since everything can be treated as string from csv
            DataType::String => u8::MAX,
            DataType::Boolean => 1,
            // everything else is not supported, thus least generality
            _ => 0,
        }
    }
}

pub struct CsvRecordReader {
    buf_reader: BufReader<File>,
    inputs_buf: String,
    outputs_buf: Vec<u8>,
    field_indices: Vec<usize>,
    csv_reader: Reader,
}

impl CsvRecordReader {
    pub fn new(path: &str) -> DBResult<Self> {
        let csv_reader = ReaderBuilder::new().build();
        // TODO: handle error properly
        let f = File::open(path)
            .map_err(|e| DBError::StorageEngine(format!("read csv file failed: {e}")))?;
        let buf_reader = BufReader::new(f);

        let inputs_buf = String::with_capacity(4096);
        let outputs_buf = [0; 4096].to_vec();
        let field_indices = [0; 256].to_vec();
        Ok(Self {
            buf_reader,
            inputs_buf,
            outputs_buf,
            field_indices,
            csv_reader,
        })
    }

    pub fn try_read_next(&mut self) -> DBResult<Option<Vec<String>>> {
        // read_line will fail when encounter invalid UTF-8 bytes
        self.inputs_buf.clear();
        let _num_read = self
            .buf_reader
            .read_line(&mut self.inputs_buf)
            .map_err(|e| DBError::StorageEngine(format!("read csv file error: {e}")))?;
        let inputs = self.inputs_buf.as_bytes();
        let (result, _num_read, _num_write, num_fields) = self.csv_reader.read_record(
            inputs,
            self.outputs_buf.as_mut_slice(),
            self.field_indices.as_mut_slice(),
        );
        match result {
            csv_core::ReadRecordResult::InputEmpty => {
                // this could happen for the last line of the csv
                unreachable!()
            }
            csv_core::ReadRecordResult::OutputFull => {
                // TODO: consider increase outputs buffer
                Err(DBError::StorageEngine(
                    "Exceeded maximum buffer size: 4096".to_string(),
                ))
            }
            csv_core::ReadRecordResult::OutputEndsFull => Err(DBError::Unknown(
                "Exceeding maximum supported num fields (256) when reading csv."
                    .to_owned(),
            )),
            csv_core::ReadRecordResult::Record => {
                let mut offset = 0;
                let record = (0..num_fields)
                    .map(|field_idx| {
                        let end = self.field_indices[field_idx];
                        // from_utf8_lossy will check the UTF-8 validity against the bytes
                        let v = String::from_utf8_lossy(&self.outputs_buf[offset..end]);
                        offset = end;
                        v.to_string()
                    })
                    .collect();
                Ok(Some(record))
            }
            csv_core::ReadRecordResult::End => Ok(None),
        }
    }
}

impl Table for CsvTable {
    fn insert_data(&mut self, _data: Vec<Row<'static>>) {
        unimplemented!()
    }

    fn get_table_meta(&self) -> TableMeta {
        TableMeta {
            schema: self.schema.clone(),
        }
    }

    fn create_scan_op(&self) -> Box<dyn PhysicalPlan> {
        Box::new(CsvScan::new(self.path.clone(), self.schema.clone()))
    }
}
