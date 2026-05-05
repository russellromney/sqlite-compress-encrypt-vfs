//! Backend-agnostic storage instrumentation.
//!
//! Turbolite no longer owns a concrete S3 client, so object-store
//! observability belongs at the `StorageBackend` boundary. Embedders and tests
//! can wrap any backend with `CountingStorageBackend`, pass the wrapper into
//! `TurboliteVfs::with_backend`, and keep the wrapper handle to inspect stats.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBackendStats {
    pub get_ops: u64,
    pub get_bytes: u64,
    pub range_get_ops: u64,
    pub range_get_bytes: u64,
    pub put_ops: u64,
    pub put_bytes: u64,
    pub put_if_absent_ops: u64,
    pub put_if_absent_bytes: u64,
    pub put_if_match_ops: u64,
    pub put_if_match_bytes: u64,
    pub put_many_ops: u64,
    pub put_many_entries: u64,
    pub put_many_bytes: u64,
    pub delete_ops: u64,
    pub delete_many_ops: u64,
    pub delete_many_entries: u64,
    pub list_ops: u64,
    pub exists_ops: u64,
}

#[derive(Default)]
struct StorageBackendCounters {
    get_ops: AtomicU64,
    get_bytes: AtomicU64,
    range_get_ops: AtomicU64,
    range_get_bytes: AtomicU64,
    put_ops: AtomicU64,
    put_bytes: AtomicU64,
    put_if_absent_ops: AtomicU64,
    put_if_absent_bytes: AtomicU64,
    put_if_match_ops: AtomicU64,
    put_if_match_bytes: AtomicU64,
    put_many_ops: AtomicU64,
    put_many_entries: AtomicU64,
    put_many_bytes: AtomicU64,
    delete_ops: AtomicU64,
    delete_many_ops: AtomicU64,
    delete_many_entries: AtomicU64,
    list_ops: AtomicU64,
    exists_ops: AtomicU64,
}

impl StorageBackendCounters {
    fn snapshot(&self) -> StorageBackendStats {
        StorageBackendStats {
            get_ops: self.get_ops.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
            range_get_ops: self.range_get_ops.load(Ordering::Relaxed),
            range_get_bytes: self.range_get_bytes.load(Ordering::Relaxed),
            put_ops: self.put_ops.load(Ordering::Relaxed),
            put_bytes: self.put_bytes.load(Ordering::Relaxed),
            put_if_absent_ops: self.put_if_absent_ops.load(Ordering::Relaxed),
            put_if_absent_bytes: self.put_if_absent_bytes.load(Ordering::Relaxed),
            put_if_match_ops: self.put_if_match_ops.load(Ordering::Relaxed),
            put_if_match_bytes: self.put_if_match_bytes.load(Ordering::Relaxed),
            put_many_ops: self.put_many_ops.load(Ordering::Relaxed),
            put_many_entries: self.put_many_entries.load(Ordering::Relaxed),
            put_many_bytes: self.put_many_bytes.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            delete_many_ops: self.delete_many_ops.load(Ordering::Relaxed),
            delete_many_entries: self.delete_many_entries.load(Ordering::Relaxed),
            list_ops: self.list_ops.load(Ordering::Relaxed),
            exists_ops: self.exists_ops.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.get_ops.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
        self.range_get_ops.store(0, Ordering::Relaxed);
        self.range_get_bytes.store(0, Ordering::Relaxed);
        self.put_ops.store(0, Ordering::Relaxed);
        self.put_bytes.store(0, Ordering::Relaxed);
        self.put_if_absent_ops.store(0, Ordering::Relaxed);
        self.put_if_absent_bytes.store(0, Ordering::Relaxed);
        self.put_if_match_ops.store(0, Ordering::Relaxed);
        self.put_if_match_bytes.store(0, Ordering::Relaxed);
        self.put_many_ops.store(0, Ordering::Relaxed);
        self.put_many_entries.store(0, Ordering::Relaxed);
        self.put_many_bytes.store(0, Ordering::Relaxed);
        self.delete_ops.store(0, Ordering::Relaxed);
        self.delete_many_ops.store(0, Ordering::Relaxed);
        self.delete_many_entries.store(0, Ordering::Relaxed);
        self.list_ops.store(0, Ordering::Relaxed);
        self.exists_ops.store(0, Ordering::Relaxed);
    }
}

pub struct CountingStorageBackend {
    inner: Arc<dyn StorageBackend>,
    counters: Arc<StorageBackendCounters>,
}

impl CountingStorageBackend {
    pub fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            counters: Arc::new(StorageBackendCounters::default()),
        }
    }

    pub fn stats(&self) -> StorageBackendStats {
        self.counters.snapshot()
    }

    pub fn reset_stats(&self) {
        self.counters.reset();
    }
}

#[async_trait]
impl StorageBackend for CountingStorageBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.counters.get_ops.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get(key).await?;
        if let Some(bytes) = &result {
            self.counters
                .get_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.counters.put_ops.fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.counters.delete_ops.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.counters.list_ops.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.counters.exists_ops.fetch_add(1, Ordering::Relaxed);
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.counters
            .put_if_absent_ops
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_if_absent_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.counters
            .put_if_match_ops
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_if_match_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put_if_match(key, data, etag).await
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.counters.range_get_ops.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.range_get(key, start, len).await?;
        if let Some(bytes) = &result {
            self.counters
                .range_get_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        self.counters
            .delete_many_ops
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .delete_many_entries
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        self.inner.delete_many(keys).await
    }

    async fn put_many(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.counters.put_many_ops.fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_many_entries
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let bytes = entries.iter().map(|(_, data)| data.len() as u64).sum();
        self.counters
            .put_many_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.inner.put_many(entries).await
    }

    fn backend_name(&self) -> &str {
        self.inner.backend_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counting_backend_tracks_object_io_without_knowing_backend_type() {
        let dir = tempfile::TempDir::new().unwrap();
        let inner: Arc<dyn StorageBackend> =
            Arc::new(hadb_storage_local::LocalStorage::new(dir.path()));
        let backend = CountingStorageBackend::new(inner);
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            backend.put("a", b"hello").await.unwrap();
            assert_eq!(backend.get("a").await.unwrap().unwrap(), b"hello");
            assert_eq!(backend.range_get("a", 1, 3).await.unwrap().unwrap(), b"ell");
            assert!(backend.exists("a").await.unwrap());
            assert_eq!(backend.list("", None).await.unwrap(), vec!["a".to_string()]);
            backend.delete("a").await.unwrap();
        });

        let stats = backend.stats();
        assert_eq!(stats.put_ops, 1);
        assert_eq!(stats.put_bytes, 5);
        assert_eq!(stats.get_ops, 1);
        assert_eq!(stats.get_bytes, 5);
        assert_eq!(stats.range_get_ops, 1);
        assert_eq!(stats.range_get_bytes, 3);
        assert_eq!(stats.exists_ops, 1);
        assert_eq!(stats.list_ops, 1);
        assert_eq!(stats.delete_ops, 1);

        backend.reset_stats();
        assert_eq!(backend.stats(), StorageBackendStats::default());
    }
}
