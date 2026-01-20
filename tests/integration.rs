//! Integration tests using the VFS with SQLite

use rusqlite::Connection;
use sqlite_compress_encrypt_vfs::{register, CompressedVfs};
use std::sync::Once;

static INIT: Once = Once::new();

fn init_vfs(dir: &std::path::Path) {
    INIT.call_once(|| {
        let vfs = CompressedVfs::new(dir, 3);
        register("compressed", vfs).expect("Failed to register VFS");
    });
}

#[test]
fn test_basic_operations() {
    let dir = tempfile::tempdir().unwrap();
    init_vfs(dir.path());

    // Open connection with our VFS
    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed",
    )
    .expect("Failed to open database");

    // Create table
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
        [],
    )
    .expect("Failed to create table");

    // Insert data
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')", [])
        .expect("Failed to insert");
    conn.execute("INSERT INTO test (id, value) VALUES (2, 'world')", [])
        .expect("Failed to insert");

    // Query data
    let mut stmt = conn.prepare("SELECT id, value FROM test ORDER BY id").unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "hello".to_string()));
    assert_eq!(rows[1], (2, "world".to_string()));
}

#[test]
fn test_large_data() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed2", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("large.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed2",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, blob BLOB)", [])
        .expect("Failed to create table");

    // Insert larger blobs
    let large_data = vec![0xABu8; 100_000];
    for i in 0..10 {
        conn.execute(
            "INSERT INTO data (id, blob) VALUES (?1, ?2)",
            rusqlite::params![i, &large_data],
        )
        .expect("Failed to insert");
    }

    // Verify
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 10);

    // Check blob integrity
    let retrieved: Vec<u8> = conn
        .query_row("SELECT blob FROM data WHERE id = 5", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retrieved, large_data);
}

#[test]
fn test_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed_wal", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("wal_test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed_wal",
    )
    .expect("Failed to open database");

    // Enable WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("Failed to set WAL mode");

    // Verify WAL mode is set
    let mode: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    println!("Journal mode: {}", mode);

    // Create table and insert data
    conn.execute("CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT)", [])
        .expect("Failed to create table");

    for i in 0..100 {
        conn.execute(
            "INSERT INTO wal_test (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, format!("row_{}", i)],
        )
        .expect("Failed to insert");
    }

    // Verify data
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM wal_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 100);

    // Checkpoint to sync WAL to main database
    let _: i64 = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
        .expect("Checkpoint failed");
}

#[test]
fn test_persistence_with_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed_persist", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("persist.db");

    // Write data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "compressed_persist",
        )
        .unwrap();

        conn.execute("CREATE TABLE persist (value TEXT)", []).unwrap();
        conn.execute("INSERT INTO persist VALUES ('test_value')", []).unwrap();
    }

    // Reopen and verify
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "compressed_persist",
        )
        .unwrap();

        let value: String = conn
            .query_row("SELECT value FROM persist", [], |r| r.get(0))
            .unwrap();
        assert_eq!(value, "test_value");
    }
}

#[test]
fn test_passthrough_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::passthrough(dir.path());
    register("passthrough_test", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("passthrough.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "passthrough_test",
    )
    .unwrap();

    conn.execute("CREATE TABLE test (id INTEGER, data TEXT)", []).unwrap();

    for i in 0..50 {
        conn.execute(
            "INSERT INTO test VALUES (?1, ?2)",
            rusqlite::params![i, format!("data_{}", i)],
        ).unwrap();
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50);
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::encrypted(dir.path(), "test-password");
    register("encrypted_test", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("encrypted.db");

    // Write data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "encrypted_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE secure (id INTEGER, secret TEXT)", []).unwrap();
        conn.execute("INSERT INTO secure VALUES (1, 'top secret')", []).unwrap();
        conn.execute("INSERT INTO secure VALUES (2, 'classified')", []).unwrap();
    }

    // Reopen and verify encryption works
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "encrypted_test",
        )
        .unwrap();

        let secrets: Vec<String> = conn
            .prepare("SELECT secret FROM secure ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(secrets, vec!["top secret", "classified"]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_compressed_encrypted_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::compressed_encrypted(dir.path(), 3, "super-secret");
    register("comp_enc_test", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("comp_enc.db");

    // Write large compressible data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "comp_enc_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE logs (id INTEGER, message TEXT)", []).unwrap();

        // Insert highly compressible repeated data
        let repeated_msg = "ERROR: connection timeout ".repeat(100);
        for i in 0..100 {
            conn.execute(
                "INSERT INTO logs VALUES (?1, ?2)",
                rusqlite::params![i, &repeated_msg],
            ).unwrap();
        }
    }

    // Reopen and verify both compression and encryption work
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "comp_enc_test",
        )
        .unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 100);

        // Verify data integrity
        let msg: String = conn
            .query_row("SELECT message FROM logs WHERE id = 50", [], |r| r.get(0))
            .unwrap();
        assert!(msg.starts_with("ERROR: connection timeout"));
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_all_four_modes_comparison() {
    let dir = tempfile::tempdir().unwrap();

    // Test data
    let test_data = "SELECT * FROM users WHERE name LIKE 'John%'".repeat(50);

    // 1. Compressed
    let vfs1 = CompressedVfs::new(dir.path(), 3);
    register("mode_comp", vfs1).unwrap();

    // 2. Passthrough
    let vfs2 = CompressedVfs::passthrough(dir.path());
    register("mode_pass", vfs2).unwrap();

    // 3. Encrypted
    let vfs3 = CompressedVfs::encrypted(dir.path(), "pwd");
    register("mode_enc", vfs3).unwrap();

    // 4. Compressed+Encrypted
    let vfs4 = CompressedVfs::compressed_encrypted(dir.path(), 3, "pwd");
    register("mode_both", vfs4).unwrap();

    // Test each mode
    for (mode_name, db_name) in [
        ("mode_comp", "comp.db"),
        ("mode_pass", "pass.db"),
        ("mode_enc", "enc.db"),
        ("mode_both", "both.db"),
    ] {
        let conn = Connection::open_with_flags_and_vfs(
            dir.path().join(db_name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            mode_name,
        )
        .unwrap();

        conn.execute("CREATE TABLE queries (sql TEXT)", []).unwrap();

        for _ in 0..20 {
            conn.execute("INSERT INTO queries VALUES (?1)", rusqlite::params![&test_data]).unwrap();
        }

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM queries", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 20);

        let retrieved: String = conn
            .query_row("SELECT sql FROM queries LIMIT 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retrieved, test_data);
    }
}
