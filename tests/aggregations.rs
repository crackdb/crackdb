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
        db.execute("select sum(amount), userId from orders group by userId"),
        Ok(expected_results)
    );
}
