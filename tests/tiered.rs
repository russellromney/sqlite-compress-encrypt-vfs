//! Integration tests for tiered S3-backed storage.
//!
//! These tests run against Tigris (S3-compatible). Requires S3 credentials.
//!
//! ```bash
//! # Source Tigris credentials, then:
//! TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo test --features tiered,zstd tiered
//! ```

#[cfg(feature = "s3")]
mod tiered {
    mod advanced;
    mod basic;
    mod borodino;
    mod btree_grouping;
    mod compact;
    mod concurrent_eviction;
    mod crash_flush;
    mod data_ops;
    mod drift;
    #[cfg(feature = "encryption")]
    mod encryption;
    mod eviction;
    mod gc;
    pub mod helpers;
    mod indexes;
    mod manifest_persistence;
    mod materialize;
    mod oracle_s3;
    mod snapshot_gc;
    mod staging;
    #[cfg(feature = "wal")]
    mod wal_integration;
}
