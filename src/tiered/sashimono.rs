//! Compact Sashimono contract vocabulary.
//!
//! This module is intentionally small and side-effect free. It pins the object
//! identity/validation rules that higher integration layers need before the
//! live VFS path starts publishing page deltas.

use serde::{Deserialize, Serialize};
use std::fmt;

pub const SASHIMONO_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    RootPointer,
    Checkpoint,
    Delta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommittedBoundary {
    AfterSqliteCommit,
    AfterWalFlush,
    AfterCheckpoint,
    ExplicitBarrier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootPointerV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub root_generation: u64,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub checkpoint_id: String,
    pub latest_delta_sequence: u64,
    pub latest_database_version: u64,
    pub state_checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub checkpoint_id: String,
    pub database_version: u64,
    pub page_size: u32,
    pub database_page_count: u64,
    pub page_objects: Vec<PageObjectRefV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageObjectRefV1 {
    pub first_page: u64,
    pub page_count: u64,
    pub key: String,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub base_database_version: u64,
    pub end_database_version: u64,
    pub sequence: u64,
    pub page_size: u32,
    pub base_database_page_count: u64,
    pub end_database_page_count: u64,
    pub committed_boundary: CommittedBoundary,
    pub changed_pages: Vec<DeltaPageV1>,
    pub checksum: u64,
    pub replay_idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaPageV1 {
    pub page_number: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    WrongKind {
        expected: ObjectKind,
        actual: ObjectKind,
    },
    UnsupportedFormatVersion(u16),
    EmptyField(&'static str),
    ZeroField(&'static str),
    RootRollback,
    StaleLeaseEpoch,
    RootIdentityMismatch,
    NonContiguousDelta {
        expected: u64,
        actual: u64,
    },
    VersionMismatch {
        expected: u64,
        actual: u64,
    },
    PageSizeMismatch {
        expected: u32,
        actual: u32,
    },
    DatabaseSizeMismatch {
        expected: u64,
        actual: u64,
    },
    DeltaPageOutOfRange {
        page_number: u64,
        end_page_count: u64,
    },
    PageObjectOutOfRange {
        first_page: u64,
        page_count: u64,
        database_page_count: u64,
    },
    DeltaPageWrongSize {
        page_number: u64,
        expected: usize,
        actual: usize,
    },
    DeltaChecksumMismatch {
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ContractError {}

pub type ContractResult<T> = Result<T, ContractError>;

impl RootPointerV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::RootPointer)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonempty("checkpoint_id", &self.checkpoint_id)?;
        require_nonzero("root_generation", self.root_generation)?;
        require_nonzero("state_checksum", self.state_checksum)?;
        Ok(())
    }

    pub fn validate_update_from(&self, previous: &RootPointerV1) -> ContractResult<()> {
        self.validate()?;
        previous.validate()?;
        if self.tenant_scope != previous.tenant_scope
            || self.database_id != previous.database_id
            || self.checkpoint_id.is_empty()
        {
            return Err(ContractError::RootIdentityMismatch);
        }
        if self.root_generation <= previous.root_generation {
            return Err(ContractError::RootRollback);
        }
        if self.lease_epoch < previous.lease_epoch {
            return Err(ContractError::StaleLeaseEpoch);
        }
        Ok(())
    }
}

impl CheckpointV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::Checkpoint)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonempty("checkpoint_id", &self.checkpoint_id)?;
        require_nonzero("database_version", self.database_version)?;
        require_nonzero("page_size", self.page_size as u64)?;
        require_nonzero("database_page_count", self.database_page_count)?;
        if self.page_objects.is_empty() {
            return Err(ContractError::EmptyField("page_objects"));
        }
        for page_object in &self.page_objects {
            page_object.validate(self.database_page_count)?;
        }
        Ok(())
    }
}

impl PageObjectRefV1 {
    pub fn validate(&self, database_page_count: u64) -> ContractResult<()> {
        require_nonempty("page_object.key", &self.key)?;
        require_nonzero("page_object.page_count", self.page_count)?;
        require_nonzero("page_object.checksum", self.checksum)?;
        let end = self.first_page.checked_add(self.page_count).ok_or(
            ContractError::PageObjectOutOfRange {
                first_page: self.first_page,
                page_count: self.page_count,
                database_page_count,
            },
        )?;
        if end > database_page_count {
            return Err(ContractError::PageObjectOutOfRange {
                first_page: self.first_page,
                page_count: self.page_count,
                database_page_count,
            });
        }
        Ok(())
    }
}

impl DeltaV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::Delta)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonzero("base_database_version", self.base_database_version)?;
        require_nonzero("end_database_version", self.end_database_version)?;
        require_nonzero("sequence", self.sequence)?;
        require_nonzero("page_size", self.page_size as u64)?;
        require_nonempty("replay_idempotency_key", &self.replay_idempotency_key)?;
        if self.end_database_version <= self.base_database_version {
            return Err(ContractError::VersionMismatch {
                expected: self.base_database_version + 1,
                actual: self.end_database_version,
            });
        }
        for page in &self.changed_pages {
            page.validate(self.page_size, self.end_database_page_count)?;
        }
        let expected = self.content_checksum();
        if self.checksum != expected {
            return Err(ContractError::DeltaChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }
        Ok(())
    }

    pub fn validate_after_root(&self, root: &RootPointerV1) -> ContractResult<()> {
        self.validate()?;
        root.validate()?;
        if self.tenant_scope != root.tenant_scope
            || self.database_id != root.database_id
            || self.lease_epoch != root.lease_epoch
        {
            return Err(ContractError::RootIdentityMismatch);
        }
        if self.sequence != root.latest_delta_sequence + 1 {
            return Err(ContractError::NonContiguousDelta {
                expected: root.latest_delta_sequence + 1,
                actual: self.sequence,
            });
        }
        if self.base_database_version != root.latest_database_version {
            return Err(ContractError::VersionMismatch {
                expected: root.latest_database_version,
                actual: self.base_database_version,
            });
        }
        Ok(())
    }

    pub fn content_checksum(&self) -> u64 {
        let mut sum = 0u64;
        sum = add_str(sum, &self.tenant_scope);
        sum = add_str(sum, &self.database_id);
        sum = add_str(sum, &self.writer_id);
        sum = sum.wrapping_add(self.lease_epoch);
        sum = sum.wrapping_add(self.base_database_version);
        sum = sum.wrapping_add(self.end_database_version);
        sum = sum.wrapping_add(self.sequence);
        sum = sum.wrapping_add(self.page_size as u64);
        sum = sum.wrapping_add(self.base_database_page_count);
        sum = sum.wrapping_add(self.end_database_page_count);
        sum = sum.wrapping_add(committed_boundary_discriminant(self.committed_boundary));
        sum = add_str(sum, &self.replay_idempotency_key);
        for page in &self.changed_pages {
            sum = sum.wrapping_add(page.page_number);
            for byte in &page.bytes {
                sum = sum.wrapping_add(*byte as u64);
            }
        }
        sum
    }
}

fn committed_boundary_discriminant(boundary: CommittedBoundary) -> u64 {
    match boundary {
        CommittedBoundary::AfterSqliteCommit => 1,
        CommittedBoundary::AfterWalFlush => 2,
        CommittedBoundary::AfterCheckpoint => 3,
        CommittedBoundary::ExplicitBarrier => 4,
    }
}

fn add_str(mut sum: u64, value: &str) -> u64 {
    for byte in value.as_bytes() {
        sum = sum.wrapping_add(*byte as u64);
    }
    sum
}

impl DeltaPageV1 {
    pub fn validate(&self, page_size: u32, end_page_count: u64) -> ContractResult<()> {
        if self.page_number >= end_page_count {
            return Err(ContractError::DeltaPageOutOfRange {
                page_number: self.page_number,
                end_page_count,
            });
        }
        let expected = page_size as usize;
        let actual = self.bytes.len();
        if actual != expected {
            return Err(ContractError::DeltaPageWrongSize {
                page_number: self.page_number,
                expected,
                actual,
            });
        }
        Ok(())
    }
}

fn validate_common(
    format_version: u16,
    tenant_scope: &str,
    database_id: &str,
    writer_id: &str,
    lease_epoch: u64,
) -> ContractResult<()> {
    if format_version != SASHIMONO_FORMAT_VERSION {
        return Err(ContractError::UnsupportedFormatVersion(format_version));
    }
    require_nonempty("tenant_scope", tenant_scope)?;
    require_nonempty("database_id", database_id)?;
    require_nonempty("writer_id", writer_id)?;
    require_nonzero("lease_epoch", lease_epoch)?;
    Ok(())
}

fn require_kind(actual: ObjectKind, expected: ObjectKind) -> ContractResult<()> {
    if actual != expected {
        return Err(ContractError::WrongKind { expected, actual });
    }
    Ok(())
}

fn require_nonempty(field: &'static str, value: &str) -> ContractResult<()> {
    if value.is_empty() {
        Err(ContractError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn require_nonzero(field: &'static str, value: u64) -> ContractResult<()> {
    if value == 0 {
        Err(ContractError::ZeroField(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT_FIXTURE: &str = include_str!("../../tests/fixtures/sashimono_root_v1.json");
    const CHECKPOINT_FIXTURE: &str =
        include_str!("../../tests/fixtures/sashimono_checkpoint_v1.json");
    const DELTA_FIXTURE: &str = include_str!("../../tests/fixtures/sashimono_delta_v1.json");

    fn root_fixture() -> RootPointerV1 {
        serde_json::from_str(ROOT_FIXTURE).expect("root fixture decodes")
    }

    fn checkpoint_fixture() -> CheckpointV1 {
        serde_json::from_str(CHECKPOINT_FIXTURE).expect("checkpoint fixture decodes")
    }

    fn delta_fixture() -> DeltaV1 {
        serde_json::from_str(DELTA_FIXTURE).expect("delta fixture decodes")
    }

    #[test]
    fn golden_root_fixture_validates() {
        root_fixture().validate().expect("root fixture validates");
    }

    #[test]
    fn golden_checkpoint_fixture_validates_and_has_no_delta_fields() {
        checkpoint_fixture()
            .validate()
            .expect("checkpoint fixture validates");

        let with_delta_field = CHECKPOINT_FIXTURE.replace(
            "\"page_objects\"",
            "\"delta_objects\": [],\n  \"page_objects\"",
        );
        let err = serde_json::from_str::<CheckpointV1>(&with_delta_field)
            .expect_err("checkpoint fixture must reject replay fields");
        assert!(
            err.to_string().contains("delta_objects"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn golden_delta_fixture_validates() {
        let delta = delta_fixture();
        assert_eq!(delta.checksum, delta.content_checksum());
        delta.validate().expect("delta fixture validates");
    }

    #[test]
    fn root_update_rejects_rollback() {
        let previous = root_fixture();
        let mut next = previous.clone();
        next.state_checksum += 1;
        let err = next
            .validate_update_from(&previous)
            .expect_err("same generation is rollback");
        assert_eq!(err, ContractError::RootRollback);
    }

    #[test]
    fn root_update_rejects_stale_lease_epoch() {
        let previous = root_fixture();
        let mut next = previous.clone();
        next.root_generation += 1;
        next.lease_epoch -= 1;
        let err = next
            .validate_update_from(&previous)
            .expect_err("stale lease must fail");
        assert_eq!(err, ContractError::StaleLeaseEpoch);
    }

    #[test]
    fn delta_rejects_wrong_identity_before_replay() {
        let root = root_fixture();
        let mut delta = delta_fixture();
        delta.tenant_scope = "tenant-b/scope-a".to_string();
        delta.checksum = delta.content_checksum();
        let err = delta
            .validate_after_root(&root)
            .expect_err("wrong tenant must fail");
        assert_eq!(err, ContractError::RootIdentityMismatch);
    }

    #[test]
    fn delta_rejects_sequence_gap() {
        let root = root_fixture();
        let mut delta = delta_fixture();
        delta.sequence += 1;
        delta.checksum = delta.content_checksum();
        let err = delta
            .validate_after_root(&root)
            .expect_err("sequence gap must fail");
        assert_eq!(
            err,
            ContractError::NonContiguousDelta {
                expected: 1,
                actual: 2
            }
        );
    }

    #[test]
    fn delta_rejects_wrong_end_database_size() {
        let mut delta = delta_fixture();
        delta.end_database_page_count = 1;
        delta.checksum = delta.content_checksum();
        let err = delta
            .validate()
            .expect_err("page beyond end page count must fail");
        assert_eq!(
            err,
            ContractError::DeltaPageOutOfRange {
                page_number: 2,
                end_page_count: 1
            }
        );
    }

    #[test]
    fn delta_rejects_corrupt_checksum() {
        let mut delta = delta_fixture();
        delta.checksum += 1;
        let err = delta.validate().expect_err("corrupt checksum must fail");
        assert!(matches!(err, ContractError::DeltaChecksumMismatch { .. }));
    }

    #[test]
    fn delta_rejects_torn_page_bytes() {
        let mut delta = delta_fixture();
        delta.changed_pages[0].bytes.pop();
        delta.checksum = delta.content_checksum();
        let err = delta.validate().expect_err("short page must fail");
        assert!(matches!(err, ContractError::DeltaPageWrongSize { .. }));
    }
}
