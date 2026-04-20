//! Per-connection install helper for `turbolite_config_set`.
//!
//! Phase Cirrus h2 — closes the multi-connection-per-thread routing
//! hole on the C side by capturing the calling connection's handle
//! queue via `sqlite3_create_function_v2`'s `pApp` pointer.
//!
//! Exported as a C-callable symbol (`turbolite_install_config_functions`)
//! so loadable-extension hosts (`SELECT load_extension(...)`), Python
//! ctypes, Go cgo, and Node koffi call the same routine. The body is in
//! Rust so it compiles in both cdylib flavors:
//!
//! - **bundled-sqlite** (standalone cdylib): sqlite3 comes in via
//!   `libsqlite3-sys`. The integration tests (which link turbolite-ffi
//!   as an rlib) exercise this path directly.
//! - **loadable-extension**: the `ext_entry.c` shim's macro-routed
//!   symbol shims (sqlite3_vfs_register, sqlite3_create_function_v2,
//!   etc.) resolve the extern "C" declarations below at link time so
//!   this code runs unchanged inside a host process.
//!
//! Call contract: invoke immediately after opening a turbolite-backed
//! connection, before opening any other turbolite connection on the
//! same thread. The helper runs `PRAGMA schema_version` internally to
//! force the VFS `xOpen`, then snapshots the top of the thread-local
//! active-handle stack — that's THIS connection's queue because install
//! runs synchronously on the opening thread.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

use crate::settings::{
    turbolite_current_queue_clone, turbolite_settings_queue_free_cb,
    turbolite_settings_queue_push,
};

// SQLite constants we need. Mirrors <sqlite3.h>.
const SQLITE_OK: c_int = 0;
const SQLITE_MISUSE: c_int = 21;
const SQLITE_UTF8: c_int = 1;
const SQLITE_DIRECTONLY: c_int = 0x0008_0000;

// Opaque handles.
#[allow(non_camel_case_types)]
pub type sqlite3 = c_void;
#[allow(non_camel_case_types)]
pub type sqlite3_context = c_void;
#[allow(non_camel_case_types)]
pub type sqlite3_value = c_void;

type CreateFnStep = unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value);
type CreateFnFinal = unsafe extern "C" fn(*mut sqlite3_context);
type CreateFnDestroy = unsafe extern "C" fn(*mut c_void);

extern "C" {
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: *const c_void,
        arg: *const c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;

    fn sqlite3_free(ptr: *mut c_void);

    fn sqlite3_create_function_v2(
        db: *mut sqlite3,
        zFunctionName: *const c_char,
        nArg: c_int,
        eTextRep: c_int,
        pApp: *mut c_void,
        xFunc: Option<CreateFnStep>,
        xStep: Option<CreateFnStep>,
        xFinal: Option<CreateFnFinal>,
        xDestroy: Option<CreateFnDestroy>,
    ) -> c_int;

    fn sqlite3_user_data(ctx: *mut sqlite3_context) -> *mut c_void;
    fn sqlite3_value_text(value: *mut sqlite3_value) -> *const c_char;
    fn sqlite3_result_error(ctx: *mut sqlite3_context, msg: *const c_char, len: c_int);
    fn sqlite3_result_int(ctx: *mut sqlite3_context, val: c_int);
}

/// Scalar function body wired via `sqlite3_create_function_v2`. Reads
/// the captured queue pointer from `pApp` (set at install time) and
/// pushes the `(key, value)` into it via the queue-FFI helpers.
unsafe extern "C" fn config_set_scalar(
    ctx: *mut sqlite3_context,
    _argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let queue = sqlite3_user_data(ctx);
    if queue.is_null() {
        let msg = b"turbolite_config_set: missing queue pointer (bug)\0";
        sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
        return;
    }

    let key = sqlite3_value_text(*argv.offset(0));
    let value = sqlite3_value_text(*argv.offset(1));
    if key.is_null() || value.is_null() {
        let msg = b"turbolite_config_set: key and value required\0";
        sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
        return;
    }

    let rc = turbolite_settings_queue_push(queue, key, value);
    if rc != 0 {
        let msg = b"turbolite_config_set: invalid key or value\0";
        sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
        return;
    }
    sqlite3_result_int(ctx, 0);
}

/// Register the `turbolite_config_set(key, value)` SQL function on this
/// connection, capturing the calling connection's handle queue via
/// `sqlite3_create_function_v2`'s `pApp`.
///
/// Returns:
/// - `SQLITE_OK` (0) on success
/// - A SQLite error code if the `PRAGMA schema_version` probe fails
///   (connection isn't turbolite-backed)
/// - `SQLITE_MISUSE` if no turbolite handle is active on this thread
///   after the probe
///
/// # Safety
/// `db` must be a live `sqlite3*` handle.
#[no_mangle]
pub unsafe extern "C" fn turbolite_install_config_functions(
    db: *mut sqlite3,
) -> c_int {
    // Force xOpen on the main-db file so THIS connection's handle
    // queue is top-of-stack on the thread-local. `PRAGMA schema_version`
    // reads page 1 which is enough to trigger the VFS open.
    let pragma = CStr::from_bytes_with_nul(b"PRAGMA schema_version\0")
        .expect("static cstring");
    let mut err_msg: *mut c_char = std::ptr::null_mut();
    let rc = sqlite3_exec(
        db,
        pragma.as_ptr(),
        std::ptr::null(),
        std::ptr::null(),
        &mut err_msg,
    );
    if !err_msg.is_null() {
        sqlite3_free(err_msg as *mut c_void);
    }
    if rc != SQLITE_OK {
        return rc;
    }

    let queue = turbolite_current_queue_clone();
    if queue.is_null() {
        return SQLITE_MISUSE;
    }

    let fn_name = CStr::from_bytes_with_nul(b"turbolite_config_set\0")
        .expect("static cstring");
    sqlite3_create_function_v2(
        db,
        fn_name.as_ptr(),
        2,
        SQLITE_UTF8 | SQLITE_DIRECTONLY,
        queue as *mut c_void,
        Some(config_set_scalar),
        None,
        None,
        Some(turbolite_settings_queue_free_cb),
    )
}
