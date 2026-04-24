//! Sync/async bridge helper.
//!
//! turbolite is a SQLite VFS and therefore synchronous from SQLite's side.
//! The `hadb_storage::StorageBackend` trait is async so that real backends
//! (S3, HTTP) can use their native async clients without a dedicated
//! tokio-per-operation. This helper lets sync call sites reach into an
//! existing tokio runtime safely in both "called from outside tokio" and
//! "called from within a tokio worker thread" contexts.

use std::future::Future;

use tokio::runtime::Handle as TokioHandle;

/// Run an async future to completion on the given tokio runtime.
///
/// Handles both cases cleanly:
/// * Called from outside a tokio context: `handle.block_on(fut)` directly.
/// * Called from inside a tokio worker (e.g. SQLite on a tokio task): use
///   `block_in_place` first so blocking the worker doesn't starve the
///   scheduler.
pub(crate) fn block_on<F, T>(handle: &TokioHandle, fut: F) -> T
where
    F: Future<Output = T>,
{
    match TokioHandle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => handle.block_on(fut),
    }
}
