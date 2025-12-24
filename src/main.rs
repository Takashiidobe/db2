use db::sql::{parse_sql, Executor};
use std::io::{self, Write};

fn main() -> io::Result<()> {
    println!("Educational SQL Database");
    println!("========================");
    println!("Commands:");
    println!("  CREATE TABLE <name> (<col1> <type>, <col2> <type>, ...)");
    println!("  CREATE INDEX <idx_name> ON <table>(<column>)");
    println!("  INSERT INTO <name> VALUES (<val1>, <val2>, ...)");
    println!("  .exit - Exit the program");
    println!();

    let mut executor = Executor::new("./data", 100)?;

    let tables = executor.list_tables();
    if tables.is_empty() {
        println!("Tables: (none loaded)");
    } else {
        println!("Tables:");
        for (name, schema) in tables {
            let cols: Vec<String> = schema
                .columns()
                .iter()
                .map(|c| format!("{} {}", c.name(), c.data_type()))
                .collect();
            println!("  - {}: {}", name, cols.join(", "));
        }
    }
    let indexes = executor.list_indexes();
    if indexes.is_empty() {
        println!("Indexes: (none loaded)");
    } else {
        println!("Indexes:");
        for (name, table, cols) in indexes {
            println!("  - {} on {}({})", name, table, cols.join(", "));
        }
    }
    println!();

    loop {
        print!("sql> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input == ".exit" {
            println!("Flushing data and exiting...");
            executor.flush_all()?;
            break;
        }

        match parse_sql(input) {
            Ok(stmt) => match executor.execute(stmt) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Execution error: {}", e),
            },
            Err(e) => eprintln!("Parse error: {}", e),
        }
    }

    Ok(())
}
