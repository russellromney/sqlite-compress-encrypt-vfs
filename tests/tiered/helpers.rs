//! Shared helpers for tiered integration tests.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::{io, path::Path};
use turbolite::tiered::{CheckpointMode, Manifest, TurboliteConfig};

/// Shared tokio runtime for all integration tests.
/// Prevents 100+ runtime creation when tests run in parallel.
static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn shared_runtime_handle() -> tokio::runtime::Handle {
    SHARED_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(16) // enough for parallel tests doing concurrent block_in_place
                .enable_all()
                .build()
                .expect("shared test runtime")
        })
        .handle()
        .clone()
}

/// Deserialize manifest from msgpack bytes into serde_json::Value for test assertions.
pub fn manifest_from_msgpack(bytes: &[u8]) -> serde_json::Value {
    let m: Manifest = rmp_serde::from_slice(bytes).expect("valid msgpack manifest");
    serde_json::to_value(&m).expect("manifest to json value")
}

/// Counter for unique VFS names across tests (SQLite requires unique names).
static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
/// Counter for unique object-store prefixes across parallel test runs.
static PREFIX_COUNTER: AtomicU32 = AtomicU32::new(0);

pub fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, n)
}

/// Get test bucket from env, or skip.
pub fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .expect("TIERED_TEST_BUCKET env var required for tiered tests")
}

/// Get the S3-compatible endpoint override, when the test environment supplies one.
///
/// Real AWS uses the provider default endpoint, so an absent value must remain
/// `None`. Tigris/S3-compatible runs provide `AWS_ENDPOINT_URL` through soup.
pub fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

pub fn aws_region() -> String {
    if endpoint_url().is_some() {
        return "auto".to_string();
    }
    std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

/// Storage tier: local-only (no S3) or S3-backed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// Pure local storage, no S3 dependency.
    Local,
    /// S3-backed with Durable sync (checkpoint uploads to S3).
    S3Durable,
    /// S3-backed with LocalThenFlush (checkpoint writes locally, explicit flush uploads).
    S3LocalThenFlush,
}

/// Test mode: combinatorial product of storage x compression x encryption.
/// 3 storage tiers x 2 compression x 2 encryption = 12 combinations.
#[derive(Debug, Clone, Copy)]
pub struct TestMode {
    pub storage: StorageTier,
    pub compressed: bool,
    pub encrypted: bool,
}

/// Deterministic test encryption key (NOT for production).
#[cfg(feature = "encryption")]
const TEST_ENCRYPTION_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];

impl TestMode {
    /// Short name for test output and unique prefixes.
    pub fn name(&self) -> String {
        let stor = match self.storage {
            StorageTier::Local => "local",
            StorageTier::S3Durable => "s3",
            StorageTier::S3LocalThenFlush => "s3ltf",
        };
        let comp = if self.compressed { "_zstd" } else { "_plain" };
        let enc = if self.encrypted { "_enc" } else { "" };
        format!("{}{}{}", stor, comp, enc)
    }

    /// Apply this mode's settings to a TurboliteConfig.
    pub fn apply(&self, config: &mut TurboliteConfig) {
        config.compression_level = if self.compressed { 3 } else { 0 };

        if self.encrypted {
            #[cfg(feature = "encryption")]
            {
                config.encryption_key = Some(TEST_ENCRYPTION_KEY);
            }
        }

        match self.storage {
            StorageTier::Local => {
                // Local mode: no S3 fields needed, test_config already sets them
                // but we override to local-only by clearing bucket
                config.cache.checkpoint_mode = CheckpointMode::Durable;
            }
            StorageTier::S3Durable => {
                config.cache.checkpoint_mode = CheckpointMode::Durable;
            }
            StorageTier::S3LocalThenFlush => {
                config.cache.checkpoint_mode = CheckpointMode::LocalThenFlush;
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_s3(&self) -> bool {
        self.storage != StorageTier::Local
    }

    pub fn supports_cold_read(&self) -> bool {
        self.storage == StorageTier::S3Durable
    }
}

/// Create a TurboliteConfig for a specific test mode with a unique prefix.
pub fn test_config_mode(
    prefix: &str,
    cache_dir: &std::path::Path,
    mode: TestMode,
) -> TurboliteConfig {
    let mut config = test_config(prefix, cache_dir);
    mode.apply(&mut config);
    config
}

/// Create a cold reader config that inherits encryption from the writer mode.
pub fn cold_reader_config_mode(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    cache_dir: &std::path::Path,
    mode: TestMode,
) -> TurboliteConfig {
    let mut config = cold_reader_config(bucket, prefix, endpoint, cache_dir);
    mode.apply(&mut config);
    // Cold readers are always read-only (already set by cold_reader_config)
    config
}

/// Create a TurboliteConfig with a unique prefix (so tests don't collide).
pub fn test_config(prefix: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let n = PREFIX_COUNTER.fetch_add(1, Ordering::SeqCst);
    let unique_prefix = format!(
        "test/{}/{}-{}-{}",
        prefix,
        std::process::id(),
        n,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    TurboliteConfig {
        bucket: test_bucket(),
        prefix: unique_prefix,
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint_url(),
        region: Some(aws_region()),
        runtime_handle: Some(shared_runtime_handle()),
        gc_enabled: false, // Async GC races with cold readers under parallel tests
        ..Default::default()
    }
}

/// Create a read-only TurboliteConfig for cold reader tests.
/// Uses the shared runtime to prevent tokio contention.
pub fn cold_reader_config(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    cache_dir: &std::path::Path,
) -> TurboliteConfig {
    TurboliteConfig {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some(aws_region()),
        read_only: true,
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    }
}

/// Build the hadb backend that replaces the old built-in S3 config path.
pub fn backend_for_config(
    config: &TurboliteConfig,
) -> (
    Arc<dyn hadb_storage::StorageBackend>,
    tokio::runtime::Handle,
) {
    let runtime = config
        .runtime_handle
        .clone()
        .unwrap_or_else(shared_runtime_handle);
    let bucket = config.bucket.clone();
    let endpoint = config.endpoint_url.clone();
    let prefix = config.prefix.clone();
    let storage = runtime
        .block_on(async { hadb_storage_s3::S3Storage::from_env(bucket, endpoint.as_deref()).await })
        .expect("create S3 storage")
        .with_prefix(prefix);
    (Arc::new(storage), runtime)
}

pub fn import_sqlite_file_compat(
    config: &TurboliteConfig,
    file_path: &Path,
) -> io::Result<Manifest> {
    let mut config = config.clone();
    config.apply_legacy_flat_fields();
    let (backend, runtime) = backend_for_config(&config);
    turbolite::tiered::import_sqlite_file(&config, backend, runtime, file_path)
}

pub fn get_manifest_compat(config: &TurboliteConfig) -> io::Result<Option<Manifest>> {
    let (backend, runtime) = backend_for_config(config);
    turbolite::tiered::get_manifest(backend.as_ref(), &runtime)
}

#[cfg(feature = "encryption")]
pub fn rotate_encryption_key_compat(
    config: &TurboliteConfig,
    new_key: Option<[u8; 32]>,
) -> io::Result<()> {
    let mut config = config.clone();
    config.apply_legacy_flat_fields();
    let (backend, runtime) = backend_for_config(&config);
    turbolite::tiered::rotate_encryption_key(&config, backend, runtime, new_key)
}

/// Directly read the S3 manifest and verify it has the expected properties.
/// This proves data actually landed in Tigris, not just local cache.
pub fn verify_s3_manifest(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    expected_page_count_min: u64,
    expected_page_size: u64,
) {
    let rt = shared_runtime_handle();
    let manifest_data = rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new(aws_region()))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let resp = client
            .get_object()
            .bucket(bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .expect("manifest should exist in S3 after checkpoint");

        resp.body.collect().await.unwrap().into_bytes().to_vec()
    });

    let manifest: serde_json::Value = manifest_from_msgpack(&manifest_data);
    let version = manifest["version"].as_u64().unwrap();
    let page_count = manifest["page_count"].as_u64().unwrap();
    let page_size = manifest["page_size"].as_u64().unwrap();
    let pages_per_group = manifest["pages_per_group"].as_u64().unwrap_or(2048);

    assert!(
        version >= 1,
        "manifest version should be >= 1, got {}",
        version
    );
    assert!(
        page_count >= expected_page_count_min,
        "manifest page_count should be >= {}, got {}",
        expected_page_count_min,
        page_count
    );
    assert_eq!(page_size, expected_page_size, "manifest page_size mismatch");
    assert!(
        pages_per_group > 0,
        "manifest pages_per_group should be > 0, got {}",
        pages_per_group
    );
}

/// Verify that S3 has page group objects under the prefix.
pub fn verify_s3_has_page_groups(bucket: &str, prefix: &str, endpoint: &Option<String>) -> usize {
    let rt = shared_runtime_handle();
    rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new(aws_region()))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(format!("{}/p/d/", prefix))
            .send()
            .await
            .expect("listing page groups should succeed");

        let count = resp.contents().len();
        assert!(count > 0, "should have at least 1 page group object in S3");
        count
    })
}

/// Helper: count all S3 objects under a prefix (pg/ + ibc/ + manifest).
pub fn count_s3_objects(bucket: &str, prefix: &str, endpoint: &Option<String>) -> usize {
    let rt = shared_runtime_handle();
    rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new(aws_region()))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let mut count = 0;
        let mut token: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let resp = req.send().await.expect("S3 list should succeed");
            count += resp.contents().len();
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        count
    })
}
