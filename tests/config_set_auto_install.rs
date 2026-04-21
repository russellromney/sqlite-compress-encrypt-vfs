//! Rust-side auto-install regression tests.
//!
//! `TurboliteVfs::new_local` registers a process-global
//! `sqlite3_auto_extension` hook (via libsqlite3-sys) the first time
//! it's called. From that point on, every `rusqlite::Connection` opened
//! in the process gets `turbolite_config_set` automatically installed
//! with its own queue captured as `pApp` — no explicit
//! `install_config_functions(&conn)` required.
//!
//! These tests open connections via `Connection::open_with_flags_and_vfs`
//! directly (bypassing `turbolite::connect`'s conveniences) and do NOT
//! call `install_config_functions`. If the auto-extension works, the
//! SQL function is present and routes to the right queue.

use std::sync::atomic::{AtomicU32, Ordering};

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use turbolite::tiered::{self, settings, TurboliteConfig, TurboliteVfs};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_name(prefix: &str) -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

/// Open a turbolite-backed connection WITHOUT calling
/// `install_config_functions`. The scalar should still be present
/// courtesy of the auto-extension hook.
fn open_bare(vfs_name: &str, db_file: &str) -> Connection {
    let conn = Connection::open_with_flags_and_vfs(
        &format!("file:{}?vfs={}", db_file, vfs_name),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open turbolite connection");
    // Force xOpen + a real page fetch so the handle's settings drain
    // path is live for the assertion at the end.
    conn.execute("CREATE TABLE IF NOT EXISTS _bootstrap (x INTEGER)", [])
        .expect("bootstrap");
    conn
}

#[test]
fn auto_install_hook_registers_turbolite_config_set_without_explicit_call() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("auto_basic");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("new_local vfs");
    tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_bare(&vfs_name, "auto.db");

    // No `install_config_functions` call. The auto-extension should
    // have bound the scalar during sqlite3_open_v2.
    let rc: i64 = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.4,0.6')",
            [],
            |row| row.get(0),
        )
        .expect("turbolite_config_set should exist via auto-extension");
    assert_eq!(rc, 0, "expected 0 from turbolite_config_set");

    let peeked = settings::peek_top_for_key("prefetch_search")
        .expect("queue should have pending update");
    assert_eq!(peeked, "0.4,0.6");
}

/// Two connections opened sequentially on the same thread: each
/// scalar's `pApp` binds to its OWN queue. Second connection's
/// `turbolite_config_set` push must NOT leak to the first.
#[test]
fn auto_install_hook_routes_per_connection() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("auto_per_conn");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("new_local vfs");
    tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn_a = open_bare(&vfs_name, "auto_a.db");
    // conn_b is held only for its lifetime effects: its open_bare pushed
    // queue_b onto the thread-local stack on top of queue_a. We never
    // read from it.
    let _conn_b = open_bare(&vfs_name, "auto_b.db");
    // Stack is now [queue_a, queue_b] — top is queue_b.

    // A pushes — pApp holds queue_a, so it lands on queue_a.
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')",
            [],
            |row| row.get(0),
        )
        .expect("A set");

    // Now push AGAIN on A and inspect top-of-stack queue_b — it must
    // NOT see A's push, proving pApp capture (not thread-local lookup).
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.55,0.44')",
            [],
            |row| row.get(0),
        )
        .expect("A set (again)");

    assert!(
        settings::peek_top_for_key("prefetch_search").is_none(),
        "queue_b (top of stack) must not carry A's pushes — auto-extension \
         must bind via pApp, not via thread-local lookup at call time"
    );
}
