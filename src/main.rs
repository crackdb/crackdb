use std::env;

use crackdb::CrackDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args:Vec<String> = env::args().skip(1).collect();
    let query = args.join(" ");
    println!("query: {}", query);
    let db = CrackDB::new();
    let rs = db.execute(query.as_str())?;
    let output = serde_json::to_string(&rs)?;
    println!("{}", output);
    Ok(())
}
