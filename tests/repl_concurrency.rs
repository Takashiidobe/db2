use std::io::Write;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

struct ReplSession {
    child: Child,
}

impl ReplSession {
    fn spawn(data_dir: &std::path::Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_db2"))
            .current_dir(data_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn repl");
        Self { child }
    }

    fn send_lines(&mut self, lines: &[&str]) {
        let stdin = self.child.stdin.as_mut().expect("stdin");
        for line in lines {
            writeln!(stdin, "{}", line).expect("write line");
        }
    }

    fn finish(mut self) -> Output {
        self.child.stdin.take();
        self.child.wait_with_output().expect("wait for repl")
    }
}

#[test]
fn test_two_repl_sessions_share_data_dir() {
    let temp_dir = TempDir::new().unwrap();

    let mut session1 = ReplSession::spawn(temp_dir.path());

    session1.send_lines(&[
        "CREATE TABLE users (id INTEGER, name VARCHAR);",
        "INSERT INTO users VALUES (1, 'Alice');",
    ]);

    let table_path = temp_dir.path().join("data").join("users.db");
    let deadline = Instant::now() + Duration::from_secs(2);
    while !table_path.exists() {
        if Instant::now() > deadline {
            panic!("table file did not appear in time");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut session2 = ReplSession::spawn(temp_dir.path());
    session2.send_lines(&["SELECT name FROM users WHERE id = 1;", ".exit"]);
    let output2 = session2.finish();
    assert!(
        output2.status.success(),
        "session2 failed: {}",
        String::from_utf8_lossy(&output2.stderr)
    );

    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    assert!(stdout2.contains("Alice"));

    session1.send_lines(&[".exit"]);
    let output1 = session1.finish();
    assert!(
        output1.status.success(),
        "session1 failed: {}",
        String::from_utf8_lossy(&output1.stderr)
    );
}
