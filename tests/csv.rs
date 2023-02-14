use std::env;

use crackdb::{
    data_types::DataType,
    expressions::Literal,
    row::Row,
    tables::{FieldInfo, RelationSchema},
    CrackDB, ResultSet,
};
use csv_core::ReaderBuilder;

#[test]
fn query_from_csv() {
    let db = CrackDB::new();
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int64),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int64(1),
            Literal::Float64(30.0),
            Literal::String("101".to_string()),
        ])],
    );
    assert_eq!(
        db.execute(
            "select id, amount, userId from 'tests/assets/orders.csv' where id = 1"
        ),
        Ok(expected_results)
    );
}

#[test]
fn query_all_from_csv() {
    let db = CrackDB::new();
    let schema = RelationSchema::new(vec![
        FieldInfo::new("userId".to_owned(), DataType::String),
        FieldInfo::new("id".to_owned(), DataType::Int64),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("dateTime".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![
            Row::new(vec![
                Literal::String("101".to_owned()),
                Literal::Int64(1),
                Literal::Float64(30.0),
                Literal::String("2023-02-14 12:35:00".to_owned()),
            ]),
            Row::new(vec![
                Literal::String("101".to_owned()),
                Literal::Int64(2),
                Literal::Float64(26.0),
                Literal::String("2023-02-14 12:36:00".to_owned()),
            ]),
            Row::new(vec![
                Literal::String("102a".to_owned()),
                Literal::Int64(3),
                Literal::Float64(64.0),
                Literal::String("2023-02-14 12:37:00".to_owned()),
            ]),
        ],
    );
    assert_eq!(
        db.execute("select userId,id, amount, dateTime from 'tests/assets/orders.csv'"),
        Ok(expected_results)
    );
}

#[test]
fn test_csv() {
    let dir = env::current_dir().unwrap();
    println!("dir: {dir:?}");
    let mut reader = ReaderBuilder::new().build();
    let input = "1,2,asdf".as_bytes();
    let mut output = [0u8; 1024];
    let mut ends = [0usize; 256];
    let (_result, num_read, num_write, num_fields) =
        reader.read_record(input, &mut output, &mut ends);
    println!("num read: {num_read}");
    println!("num write: {num_write}");
    println!("num fields: {num_fields}");
    let input = "".as_bytes();
    let (result, num_read, num_write, num_fields) =
        reader.read_record(input, &mut output, &mut ends);
    println!("num read: {num_read}");
    println!("num write: {num_write}");
    println!("num fields: {num_fields}");

    match result {
        csv_core::ReadRecordResult::InputEmpty => println!("input empty"),
        csv_core::ReadRecordResult::OutputFull => println!("output full"),
        csv_core::ReadRecordResult::OutputEndsFull => println!("output ends full"),
        csv_core::ReadRecordResult::Record => {
            println!("record: {num_fields}")
        }
        csv_core::ReadRecordResult::End => {
            println!("record end: {num_fields}")
        }
    }
}
