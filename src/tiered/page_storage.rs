//! `PageStorage`: pluggable remote-storage trait for turbolite.
//!
//! turbolite ships two built-in implementations (`Local`, `S3`). Callers that
//! need custom backends (HTTP with fence tokens, etcd, S3-compatible servers
//! with auth plugins) implement this trait and hand an `Arc<dyn PageStorage>`
//! to `TurboliteVfs::new_with_storage`.
//!
//! The trait is deliberately sync. turbolite's flush/prefetch code runs in
//! blocking worker threads — implementations that talk to async APIs wrap
//! their own runtime handles via `block_on`. This keeps turbolite free of
//! tokio assumptions.

use std::collections::HashMap;
use std::io;

use super::manifest::Manifest;

/// Remote storage backend for turbolite page groups and manifests.
///
/// The trait methods are the minimum vocabulary turbolite needs:
///   * object GET/PUT/DELETE by arbitrary key
///   * manifest GET/PUT (separate because the manifest has its own
///     serialization format and is the atomic commit point)
///   * optional byte-range GET (default implementation slices a full GET)
///   * optional parallel batch GET (default: serial)
///
/// Keys are turbolite-internal (e.g. `p/d/0_v1`, `manifest.msgpack`,
/// `p/it/3_v5`). Implementations must not rewrite them; any tenant/database
/// scoping is the implementation's own concern.
pub trait PageStorage: Send + Sync {
    /// Fetch a single page group (or other object) by key. Return `Ok(None)`
    /// if the key doesn't exist.
    fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    /// Store multiple page groups. Implementations are free to parallelize.
    fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()>;

    /// Delete objects by key. Missing keys are not an error.
    fn delete_objects(&self, keys: &[String]) -> io::Result<()>;

    /// Fetch multiple page groups in parallel. Default implementation is
    /// serial; override for backends with built-in concurrency.
    fn get_page_groups_by_key(
        &self,
        keys: &[String],
    ) -> io::Result<HashMap<String, Vec<u8>>> {
        let mut out = HashMap::new();
        for key in keys {
            if let Some(bytes) = self.get_page_group(key)? {
                out.insert(key.clone(), bytes);
            }
        }
        Ok(out)
    }

    /// Byte-range GET. Default implementation reads the full object and
    /// slices; override for backends (S3, HTTP) that support Range headers.
    fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        match self.get_page_group(key)? {
            Some(bytes) => {
                let start = start as usize;
                let end = start.saturating_add(len as usize).min(bytes.len());
                if start >= bytes.len() {
                    Ok(Some(Vec::new()))
                } else {
                    Ok(Some(bytes[start..end].to_vec()))
                }
            }
            None => Ok(None),
        }
    }

    /// Fetch the manifest object. Return `Ok(None)` if no manifest has been
    /// written yet (fresh database).
    fn get_manifest(&self) -> io::Result<Option<Manifest>>;

    /// Store the manifest. The manifest is the atomic commit point; callers
    /// rely on it appearing after all page groups that it references.
    fn put_manifest(&self, manifest: &Manifest) -> io::Result<()>;

    /// Diagnostics: number of GETs performed. Default 0; implementations
    /// that want to report telemetry can track their own counters.
    fn fetch_count(&self) -> u64 {
        0
    }

    /// Diagnostics: bytes fetched. Default 0.
    fn fetch_bytes(&self) -> u64 {
        0
    }
}
