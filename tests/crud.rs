use std::vec;

use crackdb::{
    row::{Cell, Row},
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
    let expected_results = ResultSet::new(
        vec!["id".to_owned(), "amount".to_owned(), "userId".to_owned()],
        vec![Row::new(vec![
            Cell::Int32(1),
            Cell::Int32(30),
            Cell::Int32(101),
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
    let expected_results = ResultSet::new(
        vec![
            "id".to_owned(),
            "amount".to_owned(),
            "userId".to_owned(),
            "dateTime".to_owned(),
        ],
        vec![Row::new(vec![
            Cell::Int32(1),
            Cell::Float64(30.0),
            Cell::String("101".to_string()),
            Cell::DateTime("2023-01-22 15:04:00".to_string()),
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
    let expected_results = ResultSet::new(
        vec![
            "id".to_owned(),
            "amount".to_owned(),
            "userId".to_owned(),
            "dateTime".to_owned(),
        ],
        vec![Row::new(vec![
            Cell::Int32(1),
            Cell::Float64(30.0),
            Cell::String("101".to_string()),
            Cell::DateTime("2023-01-22 15:04:00".to_string()),
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
    let expected_results = ResultSet::new(
        vec!["id".to_owned(), "amount".to_owned(), "userId".to_owned()],
        vec![Row::new(vec![
            Cell::Int32(1),
            Cell::Float64(45.0),
            Cell::String("101".to_string()),
        ])],
    );
    assert_eq!(
        db.execute(
            "select id, amount * 1.5 as amount, userId from orders where amount < 50"
        ),
        Ok(expected_results)
    );
}
