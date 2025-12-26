mod tests {
    use crate::{
        serialization::RowMetadata,
        sql::{
            ExecutionResult, Executor, IndexType, TransactionCommand, TxnState, parser::parse_sql,
        },
        table::RowId,
        types::Value,
    };
    use tempfile::TempDir;

    #[test]
    fn test_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        let result = executor.execute(stmt).unwrap();

        match result {
            ExecutionResult::CreateTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected CreateTable result"),
        }

        // Verify table exists
        assert!(executor.get_table("users").is_some());
    }

    #[test]
    fn test_create_table_duplicate() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        executor.execute(stmt).unwrap();

        // Try to create again
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Insert row
        let insert_sql = "INSERT INTO users VALUES (1, 'Alice')";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt).unwrap();

        match result {
            ExecutionResult::Insert { row_ids } => {
                assert_eq!(row_ids.len(), 1);
                let row_id = row_ids[0];
                assert_eq!(row_id.page_id(), 1); // First data page
                assert_eq!(row_id.slot_id(), 0); // First slot
            }
            _ => panic!("Expected Insert result"),
        }
    }

    #[test]
    fn test_insert_multiple_rows() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Insert multiple rows
        let inserts = vec![
            "INSERT INTO users VALUES (1, 'Alice', 30)",
            "INSERT INTO users VALUES (2, 'Bob', 25)",
            "INSERT INTO users VALUES (3, 'Charlie', 35)",
        ];

        for insert_sql in inserts {
            let stmt = parse_sql(insert_sql).unwrap();
            executor.execute(stmt).unwrap();
        }

        // Verify we can retrieve rows
        let table = executor.get_table("users").unwrap();
        let row = table.get(RowId::new(1, 0)).unwrap();
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::String("Alice".to_string()));
        assert_eq!(row[2], Value::Integer(30));
    }

    #[test]
    fn test_insert_nonexistent_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let insert_sql = "INSERT INTO nonexistent VALUES (1, 'Alice')";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_multi_tuple_single_statement() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE pairs (id INTEGER, val INTEGER)").unwrap())
            .unwrap();
        let result = executor
            .execute(parse_sql("INSERT INTO pairs VALUES (1, 2), (3, 4)").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Insert { row_ids } => {
                assert_eq!(row_ids.len(), 2);
            }
            _ => panic!("Expected Insert result"),
        }

        let result = executor
            .execute(parse_sql("SELECT * FROM pairs").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_insert_schema_validation() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Try to insert wrong number of values
        let insert_sql = "INSERT INTO users VALUES (1)";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)";
        executor.execute(parse_sql(create_sql).unwrap()).unwrap();

        // Insert products
        let products = vec![
            "INSERT INTO products VALUES (1, 'Laptop', 1000)",
            "INSERT INTO products VALUES (2, 'Mouse', 25)",
            "INSERT INTO products VALUES (3, 'Keyboard', 75)",
        ];

        for sql in products {
            executor.execute(parse_sql(sql).unwrap()).unwrap();
        }

        // Verify data
        let table = executor.get_table("products").unwrap();

        let laptop = table.get(RowId::new(1, 0)).unwrap();
        assert_eq!(laptop[1], Value::String("Laptop".to_string()));
        assert_eq!(laptop[2], Value::Integer(1000));

        let mouse = table.get(RowId::new(1, 1)).unwrap();
        assert_eq!(mouse[1], Value::String("Mouse".to_string()));
        assert_eq!(mouse[2], Value::Integer(25));
    }

    #[test]
    fn test_flush_all() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(parse_sql("CREATE TABLE test (id INTEGER)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO test VALUES (42)").unwrap())
            .unwrap();

        // Flush
        executor.flush_all().unwrap();
    }

    #[test]
    fn test_reload_tables_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        {
            let mut executor = Executor::new(temp_dir.path(), 10).unwrap();
            executor
                .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
                .unwrap();
            executor
                .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
                .unwrap();
            executor.flush_all().unwrap();
        }

        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();
        let table = executor.get_table("users").expect("table should reload");
        let row = table.get(RowId::new(1, 0)).unwrap();
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::String("Alice".to_string()));
    }

    #[test]
    fn test_select_all() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap())
            .unwrap();

        // SELECT * FROM users
        let result = executor
            .execute(parse_sql("SELECT * FROM users").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select {
                column_names, rows, ..
            } => {
                assert_eq!(column_names, vec!["id", "name"]);
                assert_eq!(rows.len(), 3);
                assert_eq!(
                    rows[0],
                    vec![Value::Integer(1), Value::String("Alice".to_string())]
                );
                assert_eq!(
                    rows[1],
                    vec![Value::Integer(2), Value::String("Bob".to_string())]
                );
                assert_eq!(
                    rows[2],
                    vec![Value::Integer(3), Value::String("Charlie".to_string())]
                );
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_columns() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(
                parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)").unwrap(),
            )
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap())
            .unwrap();

        // SELECT name, age FROM users
        let result = executor
            .execute(parse_sql("SELECT name, age FROM users").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select {
                column_names, rows, ..
            } => {
                assert_eq!(column_names, vec!["name", "age"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(
                    rows[0],
                    vec![Value::String("Alice".to_string()), Value::Integer(30)]
                );
                assert_eq!(
                    rows[1],
                    vec![Value::String("Bob".to_string()), Value::Integer(25)]
                );
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_equal() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(
                parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)").unwrap(),
            )
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie', 30)").unwrap())
            .unwrap();

        // SELECT * FROM users WHERE age = 30
        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE age = 30").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::String("Alice".to_string()));
                assert_eq!(rows[1][1], Value::String("Charlie".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_comparison() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(
                parse_sql("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)")
                    .unwrap(),
            )
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (1, 'Laptop', 1000)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (2, 'Mouse', 25)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (4, 'Monitor', 300)").unwrap())
            .unwrap();

        // SELECT * FROM products WHERE price > 100
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price > 100").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::String("Laptop".to_string()));
                assert_eq!(rows[1][1], Value::String("Monitor".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_string() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (3, 'Alice')").unwrap())
            .unwrap();

        // SELECT * FROM users WHERE name = 'Alice'
        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE name = 'Alice'").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Integer(1));
                assert_eq!(rows[1][0], Value::Integer(3));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_boolean_columns() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE flags (id INTEGER, active BOOLEAN)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO flags VALUES (1, true)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO flags VALUES (2, false)").unwrap())
            .unwrap();

        let result = executor
            .execute(parse_sql("SELECT * FROM flags WHERE active = true").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(1));
                assert_eq!(rows[0][1], Value::Boolean(true));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_empty_result() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();

        // SELECT * FROM users WHERE id = 999
        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 999").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_create_index() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();

        // Create index
        let result = executor
            .execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap())
            .unwrap();

        match result {
            ExecutionResult::CreateIndex {
                index_name,
                table_name,
                columns,
                index_type,
                is_unique,
            } => {
                assert_eq!(index_name, "idx_id");
                assert_eq!(table_name, "users");
                assert_eq!(columns, vec!["id".to_string()]);
                assert_eq!(index_type, IndexType::BTree);
                assert_eq!(is_unique, false);
            }
            _ => panic!("Expected CreateIndex result"),
        }
    }

    #[test]
    fn test_index_scan() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor
            .execute(
                parse_sql("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)")
                    .unwrap(),
            )
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (1, 'Laptop', 1000)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (2, 'Mouse', 25)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)").unwrap())
            .unwrap();

        // Create index on id
        executor
            .execute(parse_sql("CREATE INDEX idx_id ON products(id)").unwrap())
            .unwrap();

        // Query using index (WHERE id = 2)
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE id = 2").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
                assert_eq!(rows[0][1], Value::String("Mouse".to_string()));
                assert_eq!(rows[0][2], Value::Integer(25));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_index_maintained_on_insert() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table and index
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap())
            .unwrap();

        // Insert rows after creating index
        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap())
            .unwrap();

        // Query using index
        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 2").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
                assert_eq!(rows[0][1], Value::String("Bob".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_index_range_queries() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table, populate, and create index
        executor
            .execute(parse_sql("CREATE TABLE products (id INTEGER, price INTEGER)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (1, 100)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (2, 200)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (3, 300)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO products VALUES (4, 400)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("CREATE INDEX idx_price ON products(price)").unwrap())
            .unwrap();

        // Test > operator
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price > 200").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Integer(300));
                assert_eq!(rows[1][1], Value::Integer(400));
            }
            _ => panic!("Expected Select result"),
        }

        // Test >= operator
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price >= 200").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Select result"),
        }

        // Test < operator
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price < 300").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Integer(100));
                assert_eq!(rows[1][1], Value::Integer(200));
            }
            _ => panic!("Expected Select result"),
        }

        // Test <= operator
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price <= 200").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }

        // Test != operator
        let result = executor
            .execute(parse_sql("SELECT * FROM products WHERE price != 200").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_multi_column_index_prefix_scan() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE items (a INTEGER, b INTEGER, c INTEGER)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO items VALUES (1, 10, 100)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO items VALUES (1, 20, 200)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO items VALUES (2, 30, 300)").unwrap())
            .unwrap();

        executor
            .execute(parse_sql("CREATE INDEX idx_ab ON items(a, b)").unwrap())
            .unwrap();

        let result = executor
            .execute(parse_sql("SELECT a, b FROM items WHERE a = 1").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select {
                column_names,
                rows,
                plan,
            } => {
                assert_eq!(column_names, vec!["a".to_string(), "b".to_string()]);
                assert_eq!(rows.len(), 2);
                assert!(
                    plan.iter().any(|step| step.contains("Index scan")),
                    "plan did not use index: {:?}",
                    plan
                );
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_multi_column_index_with_and_predicate() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE test (id INTEGER, val INTEGER)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO test VALUES (1, 2)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO test VALUES (2, 3)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO test VALUES (4, 1)").unwrap())
            .unwrap();

        executor
            .execute(parse_sql("CREATE INDEX test_id_val ON test(id, val)").unwrap())
            .unwrap();

        let result = executor
            .execute(parse_sql("SELECT * FROM test WHERE id < 3 AND val < 3").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { rows, plan, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(plan.iter().any(|p| p.contains("id < 3 AND val < 3")));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_multi_column_index_prefix_range_plan() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO t VALUES (1, 1)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO t VALUES (5, 5)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("CREATE INDEX idx_id_val ON t(id, val)").unwrap())
            .unwrap();

        let result = executor
            .execute(parse_sql("SELECT * FROM t WHERE id < 3").unwrap())
            .unwrap();

        match result {
            ExecutionResult::Select { plan, .. } => {
                assert!(plan.iter().any(|p| p.contains("Index scan")));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_create_index_varchar_fails() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();

        // Try to create index on VARCHAR column (should fail)
        let result = executor.execute(parse_sql("CREATE INDEX idx_name ON users(name)").unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_create_index_duplicate() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table and index
        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap())
            .unwrap();

        // Try to create same index again (should fail)
        let result = executor.execute(parse_sql("CREATE INDEX idx_id2 ON users(id)").unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_join_nested_loop() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(
                parse_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)")
                    .unwrap(),
            )
            .unwrap();

        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();

        executor
            .execute(parse_sql("INSERT INTO orders VALUES (100, 1, 50)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO orders VALUES (101, 2, 20)").unwrap())
            .unwrap();

        let result = executor
            .execute(
                parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap(),
            )
            .unwrap();

        match result {
            ExecutionResult::Select {
                column_names, rows, ..
            } => {
                assert_eq!(
                    column_names,
                    vec![
                        "users.id".to_string(),
                        "users.name".to_string(),
                        "orders.id".to_string(),
                        "orders.user_id".to_string(),
                        "orders.amount".to_string(),
                    ]
                );
                assert_eq!(rows.len(), 2);
                assert!(
                    rows.iter()
                        .any(|r| r[0] == Value::Integer(1) && r[2] == Value::Integer(100))
                );
                assert!(
                    rows.iter()
                        .any(|r| r[0] == Value::Integer(2) && r[2] == Value::Integer(101))
                );
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_join_with_index_on_inner() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor
            .execute(
                parse_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)")
                    .unwrap(),
            )
            .unwrap();

        executor
            .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap())
            .unwrap();

        executor
            .execute(parse_sql("INSERT INTO orders VALUES (100, 1, 50)").unwrap())
            .unwrap();
        executor
            .execute(parse_sql("INSERT INTO orders VALUES (101, 2, 20)").unwrap())
            .unwrap();

        executor
            .execute(parse_sql("CREATE INDEX idx_orders_user_id ON orders(user_id)").unwrap())
            .unwrap();

        let result = executor.execute(parse_sql(
            "SELECT orders.amount, users.name FROM users JOIN orders ON users.id = orders.user_id",
        ).unwrap()).unwrap();

        match result {
            ExecutionResult::Select {
                column_names, rows, ..
            } => {
                assert_eq!(
                    column_names,
                    vec!["orders.amount".to_string(), "users.name".to_string()]
                );
                assert_eq!(rows.len(), 2);
                assert!(
                    rows.iter().any(|r| r[0] == Value::Integer(50)
                        && r[1] == Value::String("Alice".to_string()))
                );
                assert!(rows.iter().any(
                    |r| r[0] == Value::Integer(20) && r[1] == Value::String("Bob".to_string())
                ));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_executor_starts_outside_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let executor = Executor::new(temp_dir.path(), 10).unwrap();

        assert!(!executor.in_transaction());
    }

    #[test]
    fn test_execute_transaction_statements() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let stmt = parse_sql("BEGIN").unwrap();
        let result = executor.execute(stmt).unwrap();
        match result {
            ExecutionResult::Transaction { command } => {
                assert_eq!(command, TransactionCommand::Begin);
            }
            _ => panic!("Expected Transaction result"),
        }

        let stmt = parse_sql("COMMIT").unwrap();
        let result = executor.execute(stmt).unwrap();
        match result {
            ExecutionResult::Transaction { command } => {
                assert_eq!(command, TransactionCommand::Commit);
            }
            _ => panic!("Expected Transaction result"),
        }
    }

    #[test]
    fn test_begin_sets_transaction_state_and_rejects_nested() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        assert!(executor.in_transaction());

        let err = executor.execute(parse_sql("BEGIN").unwrap()).unwrap_err();
        assert!(err.to_string().contains("already in progress"));
    }

    #[test]
    fn test_begin_creates_snapshot_and_commit_clears_it() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let snapshot = executor.current_snapshot().expect("snapshot");
        assert!(snapshot.active.is_empty());

        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();
        assert!(executor.current_snapshot().is_none());
    }

    #[test]
    fn test_transaction_state_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let txn_id = executor.current_txn_id().expect("txn id");
        assert_eq!(executor.transaction_state(txn_id), Some(TxnState::Active));

        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();
        assert_eq!(
            executor.transaction_state(txn_id),
            Some(TxnState::Committed)
        );

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let txn_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();
        assert_eq!(executor.transaction_state(txn_id), Some(TxnState::Aborted));
    }

    #[test]
    fn test_transaction_state_recovery_from_wal() {
        let temp_dir = TempDir::new().unwrap();
        let txn_id = {
            let mut executor = Executor::new(temp_dir.path(), 10).unwrap();
            executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
            let txn_id = executor.current_txn_id().expect("txn id");
            executor.execute(parse_sql("COMMIT").unwrap()).unwrap();
            txn_id
        };

        let executor = Executor::new(temp_dir.path(), 10).unwrap();
        assert_eq!(
            executor.transaction_state(txn_id),
            Some(TxnState::Committed)
        );
    }

    #[test]
    fn test_snapshot_visibility_skips_future_xmin() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();

        let snapshot = executor.current_snapshot().expect("snapshot");
        let table = executor.get_table("users").expect("table");
        table
            .insert_with_metadata(
                &[Value::Integer(1), Value::String("Invisible".to_string())],
                RowMetadata {
                    xmin: snapshot.xmax,
                    xmax: 0,
                },
            )
            .unwrap();

        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 1").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }

    #[test]
    fn test_visibility_skips_aborted_creator() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let aborted_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(1), Value::String("Ghost".to_string())],
                    RowMetadata {
                        xmin: aborted_id,
                        xmax: 0,
                    },
                )
                .unwrap();
        }

        let result = executor
            .execute(parse_sql("SELECT * FROM users").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }

    #[test]
    fn test_visibility_hides_committed_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let creator_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let deleter_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(2), Value::String("Gone".to_string())],
                    RowMetadata {
                        xmin: creator_id,
                        xmax: deleter_id,
                    },
                )
                .unwrap();
        }

        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 2").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }

    #[test]
    fn test_visibility_ignores_aborted_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let creator_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let deleter_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(3), Value::String("Alive".to_string())],
                    RowMetadata {
                        xmin: creator_id,
                        xmax: deleter_id,
                    },
                )
                .unwrap();
        }

        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 3").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }

    #[test]
    fn test_visibility_hides_current_txn_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let creator_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let txn_id = executor.current_txn_id().expect("txn id");

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(4), Value::String("SelfDelete".to_string())],
                    RowMetadata {
                        xmin: creator_id,
                        xmax: txn_id,
                    },
                )
                .unwrap();
        }

        let result = executor
            .execute(parse_sql("SELECT * FROM users WHERE id = 4").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }

        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();
    }

    #[test]
    fn test_delete_write_conflict_aborts_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let txn_id = executor.current_txn_id().expect("txn id");
        let snapshot = executor.current_snapshot().expect("snapshot");

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(10), Value::String("Conflict".to_string())],
                    RowMetadata {
                        xmin: 0,
                        xmax: snapshot.xmax,
                    },
                )
                .unwrap();
        }

        let err = executor
            .execute(parse_sql("DELETE FROM users WHERE id = 10").unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("Write conflict"));
        assert!(!executor.in_transaction());
        assert_eq!(executor.transaction_state(txn_id), Some(TxnState::Aborted));
    }

    #[test]
    fn test_update_write_conflict_aborts_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();
        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let txn_id = executor.current_txn_id().expect("txn id");
        let snapshot = executor.current_snapshot().expect("snapshot");

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(11), Value::String("Conflict".to_string())],
                    RowMetadata {
                        xmin: 0,
                        xmax: snapshot.xmax,
                    },
                )
                .unwrap();
        }

        let err = executor
            .execute(parse_sql("UPDATE users SET name = 'X' WHERE id = 11").unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("Write conflict"));
        assert!(!executor.in_transaction());
        assert_eq!(executor.transaction_state(txn_id), Some(TxnState::Aborted));
    }

    #[test]
    fn test_vacuum_removes_dead_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor
            .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
            .unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let creator_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let aborted_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        let deleter_id = executor.current_txn_id().expect("txn id");
        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

        {
            let table = executor.get_table("users").expect("table");
            table
                .insert_with_metadata(
                    &[Value::Integer(1), Value::String("Ghost".to_string())],
                    RowMetadata {
                        xmin: aborted_id,
                        xmax: 0,
                    },
                )
                .unwrap();
            table
                .insert_with_metadata(
                    &[Value::Integer(2), Value::String("Gone".to_string())],
                    RowMetadata {
                        xmin: creator_id,
                        xmax: deleter_id,
                    },
                )
                .unwrap();
            table
                .insert_with_metadata(
                    &[Value::Integer(3), Value::String("Alive".to_string())],
                    RowMetadata {
                        xmin: creator_id,
                        xmax: 0,
                    },
                )
                .unwrap();
        }

        let removed = executor.vacuum_table("users").unwrap();
        assert_eq!(removed, 2);

        let result = executor
            .execute(parse_sql("SELECT * FROM users").unwrap())
            .unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(3));
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }

    #[test]
    fn test_commit_clears_transaction_state() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        assert!(executor.in_transaction());

        executor.execute(parse_sql("COMMIT").unwrap()).unwrap();
        assert!(!executor.in_transaction());
    }

    #[test]
    fn test_commit_without_transaction_errors() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let err = executor.execute(parse_sql("COMMIT").unwrap()).unwrap_err();
        assert!(err.to_string().contains("No active transaction"));
    }

    #[test]
    fn test_rollback_clears_transaction_state() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("BEGIN").unwrap()).unwrap();
        assert!(executor.in_transaction());

        executor.execute(parse_sql("ROLLBACK").unwrap()).unwrap();
        assert!(!executor.in_transaction());
    }

    #[test]
    fn test_rollback_without_transaction_errors() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let err = executor
            .execute(parse_sql("ROLLBACK").unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("No active transaction"));
    }
}
