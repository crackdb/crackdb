use std::env;

use crackdb::CrackDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    let args:Vec<String> = env::args().skip(1).collect();
    let query = args.join(" ");
    log::debug!("query: {}", query);
    let db = CrackDB::new();
    let rs = db.execute(query.as_str())?;
    let output = serde_json::to_string(&rs)?;
    println!("{}", output);
    Ok(())
}
