use std::io::Write;
use std::process::{Command, Stdio};

use tempfile::TempDir;

#[test]
fn test_repl_mvcc_vacuum_removes_old_versions() {
    let temp_dir = TempDir::new().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_db2"));
    command
        .current_dir(temp_dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().expect("spawn repl");
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "CREATE TABLE users (id INTEGER, name VARCHAR);").unwrap();
        writeln!(stdin, "INSERT INTO users VALUES (1, 'Alice');").unwrap();
        writeln!(stdin, "BEGIN;").unwrap();
        writeln!(stdin, "UPDATE users SET name = 'Bob' WHERE id = 1;").unwrap();
        writeln!(stdin, "COMMIT;").unwrap();
        writeln!(stdin, ".vacuum users").unwrap();
        writeln!(stdin, "SELECT name FROM users WHERE id = 1;").unwrap();
        writeln!(stdin, ".exit").unwrap();
    }

    let output = child.wait_with_output().expect("wait for repl");
    assert!(
        output.status.success(),
        "repl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Vacuum removed 1 row(s)."));
    assert!(stdout.contains("Bob"));
    assert!(!stdout.contains("Alice"));
}
