use db2::sql::{Executor, parse_sql_statements};
use std::io::{self, Write};

fn main() -> io::Result<()> {
    println!("Educational SQL Database");
    println!("========================");
    println!("Commands:");
    println!("  CREATE TABLE <name> (<col1> <type>, <col2> <type>, ...)");
    println!("  DROP TABLE <name>");
    println!("  CREATE INDEX <idx_name> ON <table>(<column>) [USING HASH]");
    println!("  BEGIN [TRANSACTION]");
    println!("  COMMIT [TRANSACTION]");
    println!("  ROLLBACK [TRANSACTION]");
    println!("  INSERT INTO <name> VALUES (<val1>, <val2>, ...)");
    println!("  UPDATE <table> SET <col> = <expr>[, ...] [WHERE <pred>]");
    println!("  DELETE FROM <name> [WHERE <pred>]");
    println!("  SELECT <cols|*> FROM <table> [WHERE <pred>] [JOIN ...]");
    println!("  .commit - Commit data to disk");
    println!("  .vacuum [table|all] - Vacuum dead row versions");
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
        for (name, table, cols, index_type, is_unique) in indexes {
            let unique_str = if is_unique { "UNIQUE " } else { "" };
            println!(
                "  - {}{} ({}) on {}({})",
                unique_str,
                name,
                index_type,
                table,
                cols.join(", ")
            );
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

        if input == ".commit" {
            println!("Flushing data and commiting...");
            if let Err(e) = executor.flush_all() {
                eprintln!("Error while flushing: {}", e);
            }
            continue;
        }

        if input == ".exit" {
            println!("Flushing data and exiting...");
            executor.flush_all()?;
            break;
        }

        if let Some(rest) = input.strip_prefix(".vacuum") {
            let target = rest.trim();
            let removed = if target.is_empty() || target.eq_ignore_ascii_case("all") {
                executor.vacuum_all()?
            } else {
                executor.vacuum_table(target)?
            };
            println!("Vacuum removed {} row(s).", removed);
            continue;
        }

        match parse_sql_statements(input) {
            Ok(stmts) => {
                for stmt in stmts {
                    match executor.execute(stmt) {
                        Ok(result) => println!("{}", result),
                        Err(e) => eprintln!("Execution error: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("Parse error: {}", e),
        }
    }

    Ok(())
}
