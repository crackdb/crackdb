use crackdb::{
    data_types::DataType,
    expressions::Literal,
    row::Row,
    tables::{FieldInfo, RelationSchema},
    CrackDB, ResultSet,
};

#[test]
pub fn support_aggregations() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 26.0, '101', '2023-02-01 20:55:00'), (3, 42.0, '102', '2023-02-01 20:55:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("sum(amount)".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![
            Row::new(vec![
                Literal::Float64(56.0),
                Literal::String("101".to_owned()),
            ]),
            Row::new(vec![
                Literal::Float64(42.0),
                Literal::String("102".to_owned()),
            ]),
        ],
    );
    assert_eq!(
        db.execute(
            "select sum(amount), userId from orders group by userId order by userId"
        ),
        Ok(expected_results)
    );
}

#[test]
pub fn support_aggregations_within_complex_expressions() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 26.0, '101', '2023-02-01 20:55:00'), (3, 42.0, '102', '2023-02-01 20:55:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![
            Row::new(vec![
                Literal::Float64(36.0),
                Literal::String("101".to_owned()),
            ]),
            Row::new(vec![
                Literal::Float64(22.0),
                Literal::String("102".to_owned()),
            ]),
        ],
    );
    assert_eq!(
        db.execute(
            "select sum(amount) - 20.0 as amount, userId from orders group by userId order by userId"
        ),
        Ok(expected_results)
    );
}

#[test]
pub fn support_more_aggregators() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 26.0, '101', '2023-02-01 20:55:00'), (3, 42.0, '102', '2023-02-01 20:55:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("avg".to_owned(), DataType::Float64),
        FieldInfo::new("max".to_owned(), DataType::Float64),
        FieldInfo::new("min".to_owned(), DataType::Float64),
        FieldInfo::new("count".to_owned(), DataType::UInt64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![
            Row::new(vec![
                Literal::Float64(28.0),
                Literal::Float64(30.0),
                Literal::Float64(26.0),
                Literal::UInt64(2),
                Literal::String("101".to_owned()),
            ]),
            Row::new(vec![
                Literal::Float64(42.0),
                Literal::Float64(42.0),
                Literal::Float64(42.0),
                Literal::UInt64(1),
                Literal::String("102".to_owned()),
            ]),
        ],
    );
    assert_eq!(
        db.execute("select avg(amount) as avg, max(amount) as max, min(amount) as min, count(*) as count, userId from orders group by userId order by userId"),
        Ok(expected_results)
    );
}

#[test]
pub fn support_having() {
    let db = CrackDB::new();
    assert_eq!(
        db.execute("create table orders (id int, amount double, userId String, dateTime DateTime)"),
        Ok(ResultSet::empty()));
    assert_eq!(
        db.execute("insert into orders values (1, 30.0, '101', '2023-01-22 15:04:00'), (2, 26.0, '101', '2023-02-01 20:55:00'), (3, 42.0, '102', '2023-02-01 20:55:00')"),
        Ok(ResultSet::empty())
    );
    let schema = RelationSchema::new(vec![
        FieldInfo::new("amount".to_owned(), DataType::Float64),
        FieldInfo::new("count".to_owned(), DataType::UInt64),
        FieldInfo::new("userId".to_owned(), DataType::String),
    ]);
    let expected_results = ResultSet::new(
        schema,
        vec![Row::new(vec![
            Literal::Float64(56.0),
            Literal::UInt64(2),
            Literal::String("101".to_owned()),
        ])],
    );
    assert_eq!(
        db.execute("select sum(amount) as amount, count(*) as count, userId from orders group by userId having avg(amount) < 30.0 order by max(amount)"),
        Ok(expected_results)
    );
}
