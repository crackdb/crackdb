pub struct CrackDB {}

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    Unknown(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResultSet {
    pub headers: Vec<String>,
    // TODO: only i32 colums are supported now
    pub rows: Vec<Vec<i32>>,
}

impl ResultSet {
    pub fn empty() -> Self {
        ResultSet {
            headers: vec![],
            rows: vec![],
        }
    }
    pub fn new(headers: Vec<String>, rows: Vec<Vec<i32>>) -> Self {
        ResultSet { headers, rows }
    }
}

impl CrackDB {
    pub fn new() -> Self {
        CrackDB {}
    }

    pub fn execute(&self, _query: &str) -> Result<ResultSet, DBError> {
        Ok(ResultSet::empty())
    }
}
