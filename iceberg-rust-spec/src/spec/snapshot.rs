//! Snapshot management and versioning for Iceberg tables.
//!
//! This module provides the core types and implementations for managing table snapshots, which
//! represent the state of a table at specific points in time. Key components include:
//!
//! - [`Snapshot`] - Represents a point-in-time state of the table
//! - [`Operation`] - Types of operations that can create new snapshots
//! - [`Summary`] - Metadata about changes made in a snapshot
//! - [`SnapshotReference`] - Named references to snapshots (branches and tags)
//! - [`SnapshotRetention`] - Policies for snapshot retention and cleanup
//!
//! Snapshots are fundamental to Iceberg's time travel and version control capabilities,
//! allowing tables to maintain their history and enabling features like rollbacks
//! and incremental processing.

use std::{
    collections::HashMap,
    fmt, str,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_builder::Builder;
use derive_getters::Getters;
use serde::{Deserialize, Serialize};

use crate::error::Error;

use _serde::SnapshotEnum;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Builder, Getters)]
#[serde(from = "SnapshotEnum", into = "SnapshotEnum")]
#[builder(build_fn(error = "Error"), setter(prefix = "with"))]
/// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
pub struct Snapshot {
    /// A unique long ID
    #[builder(default = "generate_snapshot_id()")]
    snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    #[builder(setter(strip_option), default)]
    parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    #[builder(
        default = "SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64"
    )]
    timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    #[builder(default)]
    summary: Summary,
    /// ID of the table’s current schema when the snapshot was created.
    #[builder(setter(strip_option), default)]
    schema_id: Option<i32>,
}

/// Generates a random snapshot ID using a cryptographically secure random number generator.
///
/// The function generates 8 random bytes and converts them to a positive i64 value.
/// This ensures unique snapshot IDs across the table's history.
pub fn generate_snapshot_id() -> i64 {
    let mut bytes: [u8; 8] = [0u8; 8];
    getrandom::fill(&mut bytes).unwrap();
    i64::from_le_bytes(bytes).abs()
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for Snapshot {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

pub(crate) mod _serde {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::{Operation, Snapshot, Summary};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum SnapshotEnum {
        V2(SnapshotV2),
        V1(SnapshotV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    #[serde(rename_all = "kebab-case")]
    /// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
    pub struct SnapshotV2 {
        /// A unique long ID
        pub snapshot_id: i64,
        /// The snapshot ID of the snapshot’s parent.
        /// Omitted for any snapshot with no parent
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        /// A monotonically increasing long that tracks the order of
        /// changes to a table.
        pub sequence_number: i64,
        /// A timestamp when the snapshot was created, used for garbage
        /// collection and table inspection
        pub timestamp_ms: i64,
        /// The location of a manifest list for this snapshot that
        /// tracks manifest files with additional metadata.
        pub manifest_list: String,
        /// A string map that summarizes the snapshot changes, including operation.
        pub summary: Summary,
        /// ID of the table’s current schema when the snapshot was created.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<i32>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    #[serde(rename_all = "kebab-case")]
    /// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
    pub struct SnapshotV1 {
        /// A unique long ID
        pub snapshot_id: i64,
        /// The snapshot ID of the snapshot’s parent.
        /// Omitted for any snapshot with no parent
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        /// A timestamp when the snapshot was created, used for garbage
        /// collection and table inspection
        pub timestamp_ms: i64,
        /// The location of a manifest list for this snapshot that
        /// tracks manifest files with additional metadata.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifest_list: Option<String>,
        /// A list of manifest file locations. Must be omitted if manifest-list is present
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifests: Option<Vec<String>>,
        /// A string map that summarizes the snapshot changes, including operation.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<Summary>,
        /// ID of the table’s current schema when the snapshot was created.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<i32>,
    }
    impl From<SnapshotEnum> for Snapshot {
        fn from(value: SnapshotEnum) -> Self {
            match value {
                SnapshotEnum::V2(value) => value.into(),
                SnapshotEnum::V1(value) => value.into(),
            }
        }
    }

    impl From<Snapshot> for SnapshotEnum {
        fn from(value: Snapshot) -> Self {
            SnapshotEnum::V2(value.into())
        }
    }

    impl From<SnapshotV1> for Snapshot {
        fn from(v1: SnapshotV1) -> Self {
            Snapshot {
                snapshot_id: v1.snapshot_id,
                parent_snapshot_id: v1.parent_snapshot_id,
                sequence_number: 0,
                timestamp_ms: v1.timestamp_ms,
                manifest_list: v1.manifest_list.unwrap_or_default(),
                summary: v1.summary.unwrap_or(Summary {
                    operation: Operation::default(),
                    other: HashMap::new(),
                }),
                schema_id: v1.schema_id,
            }
        }
    }

    impl From<Snapshot> for SnapshotV1 {
        fn from(v1: Snapshot) -> Self {
            SnapshotV1 {
                snapshot_id: v1.snapshot_id,
                parent_snapshot_id: v1.parent_snapshot_id,
                timestamp_ms: v1.timestamp_ms,
                manifest_list: Some(v1.manifest_list),
                summary: Some(v1.summary),
                schema_id: v1.schema_id,
                manifests: None,
            }
        }
    }

    impl From<SnapshotV2> for Snapshot {
        fn from(value: SnapshotV2) -> Self {
            Snapshot {
                snapshot_id: value.snapshot_id,
                parent_snapshot_id: value.parent_snapshot_id,
                sequence_number: value.sequence_number,
                timestamp_ms: value.timestamp_ms,
                manifest_list: value.manifest_list,
                summary: value.summary,
                schema_id: value.schema_id,
            }
        }
    }

    impl From<Snapshot> for SnapshotV2 {
        fn from(value: Snapshot) -> Self {
            SnapshotV2 {
                snapshot_id: value.snapshot_id,
                parent_snapshot_id: value.parent_snapshot_id,
                sequence_number: value.sequence_number,
                timestamp_ms: value.timestamp_ms,
                manifest_list: value.manifest_list,
                summary: value.summary,
                schema_id: value.schema_id,
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
/// The operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots.
#[derive(Default)]
pub enum Operation {
    /// Only data files were added and no files were removed.
    #[default]
    Append,
    /// Data and delete files were added and removed without changing table data;
    /// i.e., compaction, changing the data file format, or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    Delete,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Operation,
    /// Other summary data.
    #[serde(flatten)]
    pub other: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// Snapshot retention policy
    pub retention: SnapshotRetention,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
/// The snapshot expiration procedure removes snapshots from table metadata and applies the table’s retention policy.
pub enum SnapshotRetention {
    #[serde(rename_all = "kebab-case")]
    /// Branches are mutable named references that can be updated by committing a new snapshot as
    /// the branch’s referenced snapshot using the Commit Conflict Resolution and Retry procedures.
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a branch while expiring snapshots.
        /// Defaults to table property history.expire.min-snapshots-to-keep.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        /// A positive number for the max age of snapshots to keep when expiring, including the latest snapshot.
        /// Defaults to table property history.expire.max-snapshot-age-ms.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    /// Tags are labels for individual snapshots.
    Tag {
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        max_ref_age_ms: i64,
    },
}

impl Default for SnapshotRetention {
    fn default() -> Self {
        SnapshotRetention::Branch {
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }
}
