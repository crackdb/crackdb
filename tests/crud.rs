use std::vec;

use crackdb::{
    data_types::DataType,
    expressions::Literal,
    row::Row,
    tables::{FieldInfo, RelationSchema},
    CrackDB, ResultSet,
};

#[test]
fn create_insert_and_query() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount int, userId int)"),
        Ok(ResultSet::empty())
    );
    assert_eq!(
        db.execute("insert into orders values (1, 30, 101)"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Int32),
        FieldInfo::new("userId".to_owned(), DataType::Int32),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int32(1),
            Literal::Int32(30),
            Literal::Int32(101),
        ])],
    );
    assert_eq!(
        db.execute("select * from orders where id > 0"),
        Ok(expected_results)
    );
}

#[test]
fn create_insert_and_query_with_more_data_types_and_expressions() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty())
    );
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
        FieldInfo::new("dateTime".to_owned(), DataType::DateTime),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int32(1),
            Literal::Float64(30.0),
            Literal::String("101".to_string()),
            Literal::DateTime("2023-01-22 15:04:00".to_string()),
        ])],
    );
    assert_eq!(
        db.execute("select * from orders where id = 1"),
        Ok(expected_results)
    );
}

#[test]
fn create_insert_and_query_with_more_expressions() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00')"),
        Ok(ResultSet::empty())
    );
    assert_eq!(
        db.execute("insert into orders values (2, 60.0, '102', '2023-01-24 21:07:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
        FieldInfo::new("dateTime".to_owned(), DataType::DateTime),
    ]);

    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int32(1),
            Literal::Float64(30.0),
            Literal::String("101".to_string()),
            Literal::DateTime("2023-01-22 15:04:00".to_string()),
        ])],
    );
    assert_eq!(
        db.execute("select * from orders where amount * 1.5 > 40.0 and amount < 50"),
        Ok(expected_results)
    );
}

#[test]
fn supports_projection_query() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int32(1),
            Literal::Float64(45.0),
            Literal::String("101".to_string()),
        ])],
    );
    assert_eq!(
        db.execute(
            "select id, amount * 1.5 as amount, userId from orders where amount < 50"
        ),
        Ok(expected_results)
    );
}

#[test]
fn support_order_by() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 60.0, '103', '2023-01-24 21:07:00'), (3, 60.0, '102', '2023-01-24 21:07:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);

    let expected_results = ResultSet::new(
        schema,
        vec![
            Row::new(vec![
                Literal::Int32(1),
                Literal::Float64(30.0),
                Literal::String("101".to_string()),
            ]),
            Row::new(vec![
                Literal::Int32(3),
                Literal::Float64(60.0),
                Literal::String("102".to_string()),
            ]),
            Row::new(vec![
                Literal::Int32(2),
                Literal::Float64(60.0),
                Literal::String("103".to_string()),
            ]),
        ],
    );
    assert_eq!(
        db.execute("select id, amount, userId from orders order by userId"),
        Ok(expected_results)
    );
}

#[test]
fn support_limit_and_offset() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 60.0, '103', '2023-01-24 21:07:00'), (3, 60.0, '102', '2023-01-24 21:07:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("id".to_owned(), DataType::Int32),
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
        FieldInfo::new("dateTime".to_owned(), DataType::DateTime),
    ]);

    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Int32(3),
            Literal::Float64(60.0),
            Literal::String("102".to_string()),
            Literal::DateTime("2023-01-24 21:07:00".to_string()),
        ])],
    );
    assert_eq!(
        db.execute("select * from orders order by userId limit 1 offset 1"),
        Ok(expected_results)
    );
}
