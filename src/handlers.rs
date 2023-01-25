use sqlparser::ast::Statement;

use crate::{DBResult, ResultSet};

mod create_table_handler;
mod insert_handler;
mod select_handler;
pub use create_table_handler::CreateTableHandler;
pub use insert_handler::InsertHandler;
pub use select_handler::SelectHandler;

pub trait QueryHandler {
    fn handle(&self, statement: Statement) -> DBResult<ResultSet>;
}
