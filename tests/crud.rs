use std::vec;

use crackdb::{CrackDB, ResultSet};

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
        vec![vec![1, 30, 101]],
    );
    assert_eq!(
        db.execute("select * from orders where id > 1"),
        Ok(expected_results)
    );
}
