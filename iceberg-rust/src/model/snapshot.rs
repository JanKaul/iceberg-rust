/**
 * Snapshots
*/
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// The operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots.
pub enum Operation {
    /// Only data files were added and no files were removed.
    Append,
    /// Data and delete files were added and removed without changing table data;
    /// i.e., compaction, changing the data file format, or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    Delete,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Operation,
    /// Other summary data.
    #[serde(flatten)]
    pub other: HashMap<String, String>,
}

impl Default for Operation {
    fn default() -> Operation {
        Self::Append
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    pub schema_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    pub schema_id: Option<i64>,
}

impl From<SnapshotV1> for SnapshotV2 {
    fn from(v1: SnapshotV1) -> Self {
        SnapshotV2 {
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct Reference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// Snapshot retention policy
    pub retention: Retention,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase", tag = "type")]
/// The snapshot expiration procedure removes snapshots from table metadata and applies the table’s retention policy.
pub enum Retention {
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
