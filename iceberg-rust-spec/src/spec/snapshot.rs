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
        #[serde(skip_serializing_if = "Option::is_none", default)]
        max_ref_age_ms: Option<i64>,
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Snapshot --------------------------------------------------------

    #[test]
    fn test_snapshot_v2_json_round_trip_with_full_population() {
        let json = r#"{
            "snapshot-id": 8390124917451423234,
            "parent-snapshot-id": 8390124917451423233,
            "sequence-number": 17,
            "timestamp-ms": 1730000000000,
            "manifest-list": "s3://bucket/db/table/metadata/snap-17-1-uuid.avro",
            "summary": {
                "operation": "append",
                "added-data-files": "3",
                "added-records": "150"
            },
            "schema-id": 4
        }"#;

        let snap: Snapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.snapshot_id(), &8390124917451423234_i64);
        assert_eq!(snap.parent_snapshot_id(), &Some(8390124917451423233));
        assert_eq!(snap.sequence_number(), &17_i64);
        assert_eq!(snap.timestamp_ms(), &1730000000000_i64);
        assert_eq!(
            snap.manifest_list(),
            "s3://bucket/db/table/metadata/snap-17-1-uuid.avro",
        );
        assert_eq!(snap.summary().operation, Operation::Append);
        assert_eq!(
            snap.summary().other.get("added-records"),
            Some(&"150".to_string())
        );
        assert_eq!(snap.schema_id(), &Some(4));

        let again: Snapshot = serde_json::from_str(&serde_json::to_string(&snap).unwrap()).unwrap();
        assert_eq!(again, snap);

        // Display/FromStr exercises the same Serialize/Deserialize impls via
        // String I/O.
        let parsed: Snapshot = snap.to_string().parse().unwrap();
        assert_eq!(parsed, snap);
    }

    #[test]
    fn test_snapshot_without_parent_is_serialized_without_parent_field() {
        let json = r#"{
            "snapshot-id": 1,
            "sequence-number": 1,
            "timestamp-ms": 1730000000000,
            "manifest-list": "snap-1.avro",
            "summary": { "operation": "append" }
        }"#;

        let snap: Snapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.parent_snapshot_id(), &None);

        let serialized = serde_json::to_string(&snap).unwrap();
        assert!(
            !serialized.contains("parent-snapshot-id"),
            "root snapshot must not emit `parent-snapshot-id`; got {serialized}",
        );
    }

    #[test]
    fn test_snapshot_v1_input_defaults_sequence_number_to_zero() {
        // V1 snapshots predate sequence numbers. When deserializing V1 JSON
        // through the V1/V2 enum, the resulting Snapshot must have
        // `sequence_number = 0`.
        let v1_json = r#"{
            "snapshot-id": 100,
            "timestamp-ms": 1730000000000,
            "manifest-list": "snap-100.avro"
        }"#;
        let snap: Snapshot = serde_json::from_str(v1_json).unwrap();
        assert_eq!(snap.sequence_number(), &0);
        assert_eq!(snap.snapshot_id(), &100);
        assert_eq!(snap.summary().operation, Operation::default());
    }

    #[test]
    fn test_snapshot_summary_other_entries_round_trip_via_flatten() {
        let mut other = HashMap::new();
        other.insert("added-data-files".to_string(), "7".to_string());
        other.insert("changed-partition-count".to_string(), "2".to_string());

        let summary = Summary {
            operation: Operation::Overwrite,
            other,
        };
        let json = serde_json::to_string(&summary).unwrap();
        // `operation` is rendered at the top level because of the rename_all
        // = lowercase serde attribute on Operation; the `other` map is
        // flattened next to it.
        assert!(json.contains("\"operation\":\"overwrite\""));
        assert!(json.contains("\"added-data-files\":\"7\""));

        let parsed: Summary = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, summary);
    }

    #[test]
    fn test_operation_serializes_as_lowercase_keyword_per_spec() {
        for (op, expected_token) in [
            (Operation::Append, "\"append\""),
            (Operation::Replace, "\"replace\""),
            (Operation::Overwrite, "\"overwrite\""),
            (Operation::Delete, "\"delete\""),
        ] {
            let json = serde_json::to_string(&op).unwrap();
            assert_eq!(json, expected_token, "Operation::{op:?}");
            let parsed: Operation = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, op);
        }
    }

    // --- TestSnapshotSummary additional coverage ---------------------------
    //
    // The spec defines a small lowercase enum for the snapshot operation
    // and a key/value summary map populated with canonical metric keys
    // like `added-records`, `total-data-files`, etc. The tests below pin
    // the default state, parser strictness, and the canonical key shape.

    #[test]
    fn test_operation_default_is_append() {
        assert_eq!(Operation::default(), Operation::Append);
    }

    #[test]
    fn test_summary_default_has_append_operation_and_empty_other() {
        let summary = Summary::default();
        assert_eq!(summary.operation, Operation::Append);
        assert!(
            summary.other.is_empty(),
            "default Summary should not carry pre-populated metrics",
        );
    }

    #[test]
    fn test_summary_parser_rejects_unknown_operation_keyword() {
        let json = r#"{
            "operation": "rewrite-manifests"
        }"#;
        assert!(serde_json::from_str::<Summary>(json).is_err());
    }

    #[test]
    fn test_summary_parser_rejects_uppercase_operation_keyword() {
        // The serde rename_all = "lowercase" attribute on Operation makes
        // the parser case-sensitive; uppercase keywords must not match.
        let json = r#"{
            "operation": "APPEND"
        }"#;
        assert!(serde_json::from_str::<Summary>(json).is_err());
    }

    #[test]
    fn test_summary_round_trips_common_spec_metric_keys() {
        // The canonical key set produced by Iceberg writers on an append
        // commit. The Rust Summary models these as `other: HashMap<...>`,
        // and the test pins that every common key survives a JSON
        // round-trip without being absorbed into a typed field.
        let canonical_keys: &[(&str, &str)] = &[
            ("added-data-files", "3"),
            ("added-records", "150"),
            ("added-files-size", "1024000"),
            ("removed-data-files", "0"),
            ("removed-records", "0"),
            ("total-data-files", "12"),
            ("total-records", "1850"),
            ("total-files-size", "4096000"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "0"),
        ];

        let mut other = HashMap::new();
        for (k, v) in canonical_keys {
            other.insert((*k).to_owned(), (*v).to_owned());
        }
        let summary = Summary {
            operation: Operation::Append,
            other: other.clone(),
        };

        let parsed: Summary =
            serde_json::from_str(&serde_json::to_string(&summary).unwrap()).unwrap();
        assert_eq!(parsed.operation, Operation::Append);
        for (k, v) in canonical_keys {
            assert_eq!(
                parsed.other.get(*k).map(String::as_str),
                Some(*v),
                "round-trip lost key {k}",
            );
        }
    }

    #[test]
    fn test_summary_with_only_operation_parses_to_default_other() {
        // A summary that omits all metric keys must still parse and yield
        // an empty `other` map.
        let json = r#"{
            "operation": "delete"
        }"#;
        let parsed: Summary = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.operation, Operation::Delete);
        assert!(parsed.other.is_empty());
    }

    // --- SnapshotReference & SnapshotRetention --------------------------

    #[test]
    fn test_snapshot_reference_branch_with_all_retention_fields_round_trip() {
        let json = r#"{
            "snapshot-id": 42,
            "type": "branch",
            "min-snapshots-to-keep": 5,
            "max-snapshot-age-ms": 86400000,
            "max-ref-age-ms": 604800000
        }"#;

        let r: SnapshotReference = serde_json::from_str(json).unwrap();
        assert_eq!(r.snapshot_id, 42);
        match &r.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            } => {
                assert_eq!(*min_snapshots_to_keep, Some(5));
                assert_eq!(*max_snapshot_age_ms, Some(86_400_000));
                assert_eq!(*max_ref_age_ms, Some(604_800_000));
            }
            other => panic!("expected branch retention, got {other:?}"),
        }

        let again: SnapshotReference =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(again, r);
    }

    #[test]
    fn test_snapshot_reference_branch_omits_unset_retention_fields() {
        // Default branch retention has all three policy fields unset; the
        // serialized JSON must therefore carry only `snapshot-id` and `type`.
        let r = SnapshotReference {
            snapshot_id: 11,
            retention: SnapshotRetention::default(),
        };
        let serialized = serde_json::to_string(&r).unwrap();
        assert!(serialized.contains("\"type\":\"branch\""));
        assert!(!serialized.contains("min-snapshots-to-keep"));
        assert!(!serialized.contains("max-snapshot-age-ms"));
        assert!(!serialized.contains("max-ref-age-ms"));

        let parsed: SnapshotReference = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed, r);
    }

    #[test]
    fn test_snapshot_reference_tag_round_trip() {
        let json = r#"{
            "snapshot-id": 99,
            "type": "tag",
            "max-ref-age-ms": 1234
        }"#;

        let r: SnapshotReference = serde_json::from_str(json).unwrap();
        assert_eq!(r.snapshot_id, 99);
        match r.retention {
            SnapshotRetention::Tag { max_ref_age_ms } => {
                assert_eq!(max_ref_age_ms, Some(1234))
            }
            other => panic!("expected tag retention, got {other:?}"),
        }

        let again: SnapshotReference =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(again, r);
    }

    #[test]
    fn test_snapshot_retention_default_is_unbounded_branch() {
        assert_eq!(
            SnapshotRetention::default(),
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        );
    }

    #[test]
    fn test_snapshot_reference_tag_allows_omitting_max_ref_age_ms() {
        // Spec/Java allow a tag with only `snapshot-id` and `type`. The Rust
        // variant treats `max_ref_age_ms` as optional in line with that
        // contract, and re-serialising omits the absent field.
        let json = r#"{
            "snapshot-id": 1,
            "type": "tag"
        }"#;

        let r: SnapshotReference = serde_json::from_str(json).unwrap();
        assert_eq!(r.snapshot_id, 1);
        match r.retention {
            SnapshotRetention::Tag { max_ref_age_ms } => assert_eq!(max_ref_age_ms, None),
            other => panic!("expected tag retention, got {other:?}"),
        }

        let again = serde_json::to_value(&r).unwrap();
        assert!(again.get("max-ref-age-ms").is_none(), "got {again}");
        let round_trip: SnapshotReference = serde_json::from_value(again).unwrap();
        assert_eq!(round_trip, r);
    }
}
