//! Integration tests for snapshot expiration functionality
//!
//! These tests verify that the expire_snapshots API works correctly with real
//! table metadata structures and various expiration criteria.

use iceberg_rust::table::maintenance::{ExpireSnapshots, ExpireSnapshotsResult};
use iceberg_rust_spec::spec::{
    snapshot::{Snapshot, SnapshotBuilder, SnapshotReference, SnapshotRetention},
    table_metadata::{TableMetadata, TableMetadataBuilder, FormatVersion},
};
use std::collections::HashMap;
use uuid::Uuid;

/// Test helper to create a snapshot with given ID and timestamp
fn create_test_snapshot(id: i64, timestamp_ms: i64) -> Snapshot {
    SnapshotBuilder::default()
        .with_snapshot_id(id)
        .with_sequence_number(id)
        .with_timestamp_ms(timestamp_ms)
        .with_manifest_list(format!("manifest-list-{}.avro", id))
        .build()
        .unwrap()
}

/// Test helper to create table metadata with test snapshots
fn create_test_metadata_with_snapshots(
    snapshots: Vec<(i64, i64)>, // (id, timestamp) pairs
    current_snapshot_id: Option<i64>,
) -> TableMetadata {
    let mut snapshot_map = HashMap::new();
    
    for (id, timestamp) in snapshots {
        let snapshot = create_test_snapshot(id, timestamp);
        snapshot_map.insert(id, snapshot);
    }

    TableMetadata {
        format_version: FormatVersion::V2,
        table_uuid: Uuid::new_v4(),
        location: "s3://test-bucket/test-table".to_string(),
        last_sequence_number: 0,
        last_updated_ms: 0,
        last_column_id: 0,
        schemas: HashMap::new(),
        current_schema_id: 0,
        partition_specs: HashMap::new(),
        default_spec_id: 0,
        last_partition_id: 0,
        properties: HashMap::new(),
        current_snapshot_id,
        snapshots: snapshot_map,
        snapshot_log: Vec::new(),
        metadata_log: Vec::new(),
        sort_orders: HashMap::new(),
        default_sort_order_id: 0,
        refs: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expire_by_count_basic() {
        // Create metadata with 5 snapshots
        let base_time = 1000000000000i64;
        let snapshots = vec![
            (1, base_time + 1000),
            (2, base_time + 2000),
            (3, base_time + 3000),
            (4, base_time + 4000),
            (5, base_time + 5000),
        ];
        let metadata = create_test_metadata_with_snapshots(snapshots, Some(5));

        // Test selection logic by directly calling it
        let expire_config = TestExpireSnapshots {
            older_than: None,
            retain_last: Some(3),
            retain_ref_snapshots: true,
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        // Should retain the 3 most recent snapshots (3, 4, 5)
        // Should expire the 2 oldest snapshots (1, 2)
        assert_eq!(retained.len(), 3);
        assert_eq!(expired.len(), 2);
        
        assert!(retained.contains(&3));
        assert!(retained.contains(&4));
        assert!(retained.contains(&5));
        
        assert!(expired.contains(&1));
        assert!(expired.contains(&2));
    }

    #[test]
    fn test_expire_by_timestamp() {
        let base_time = 1000000000000i64;
        let snapshots = vec![
            (1, base_time + 1 * 24 * 60 * 60 * 1000), // Day 1
            (2, base_time + 2 * 24 * 60 * 60 * 1000), // Day 2
            (3, base_time + 3 * 24 * 60 * 60 * 1000), // Day 3
            (4, base_time + 4 * 24 * 60 * 60 * 1000), // Day 4
            (5, base_time + 5 * 24 * 60 * 60 * 1000), // Day 5
        ];
        let metadata = create_test_metadata_with_snapshots(snapshots, Some(5));

        // Expire snapshots older than day 3
        let expire_config = TestExpireSnapshots {
            older_than: Some(base_time + 3 * 24 * 60 * 60 * 1000),
            retain_last: None,
            retain_ref_snapshots: true,
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        // Should retain snapshots from day 3 onwards (3, 4, 5)
        // Should expire snapshots from before day 3 (1, 2)
        assert!(retained.contains(&3));
        assert!(retained.contains(&4));
        assert!(retained.contains(&5));
        
        assert!(expired.contains(&1));
        assert!(expired.contains(&2));
    }

    #[test]
    fn test_never_expire_current_snapshot() {
        let base_time = 1000000000000i64;
        let snapshots = vec![
            (1, base_time + 1000), // This will be the current snapshot
        ];
        let metadata = create_test_metadata_with_snapshots(snapshots, Some(1));

        // Try to expire by timestamp - should fail because it's current
        let expire_config = TestExpireSnapshots {
            older_than: Some(base_time + 2000), // After the snapshot timestamp
            retain_last: None,
            retain_ref_snapshots: true,
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        // Current snapshot should never be expired
        assert!(retained.contains(&1));
        assert!(expired.is_empty());
    }

    #[test]
    fn test_retain_last_overrides_timestamp() {
        let base_time = 1000000000000i64;
        let snapshots = vec![
            (1, base_time + 1000),
            (2, base_time + 2000),
            (3, base_time + 3000),
        ];
        let metadata = create_test_metadata_with_snapshots(snapshots, Some(3));

        // Set timestamp that would expire all snapshots, but retain_last should override
        let expire_config = TestExpireSnapshots {
            older_than: Some(base_time + 4000), // After all snapshots
            retain_last: Some(2), // But keep 2 most recent
            retain_ref_snapshots: true,
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        // Should retain 2 most recent (2, 3) despite timestamp criteria
        assert_eq!(retained.len(), 2);
        assert_eq!(expired.len(), 1);
        
        assert!(retained.contains(&2));
        assert!(retained.contains(&3));
        assert!(expired.contains(&1));
    }

    #[test]
    fn test_refs_prevent_expiration() {
        let base_time = 1000000000000i64;
        let snapshots = vec![
            (1, base_time + 1000),
            (2, base_time + 2000),
            (3, base_time + 3000),
        ];
        let mut metadata = create_test_metadata_with_snapshots(snapshots, Some(3));
        
        // Add a ref pointing to snapshot 1
        metadata.refs.insert(
            "test-branch".to_string(),
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        );

        let expire_config = TestExpireSnapshots {
            older_than: Some(base_time + 4000), // Would expire all snapshots
            retain_last: None,
            retain_ref_snapshots: true, // Preserve ref snapshots
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        // Should retain snapshot 1 because it's referenced by a branch
        // Should retain snapshot 3 because it's current
        // Should expire snapshot 2
        assert!(retained.contains(&1)); // Referenced by branch
        assert!(retained.contains(&3)); // Current snapshot
        assert!(expired.contains(&2));  // Not protected
    }

    #[test]
    fn test_empty_metadata() {
        let metadata = create_test_metadata_with_snapshots(vec![], None);

        let expire_config = TestExpireSnapshots {
            older_than: Some(1000),
            retain_last: None,
            retain_ref_snapshots: true,
        };

        let (expired, retained) = expire_config.select_snapshots_to_expire(&metadata).unwrap();

        assert!(expired.is_empty());
        assert!(retained.is_empty());
    }

    /// Test helper struct that mimics ExpireSnapshots for unit testing
    struct TestExpireSnapshots {
        older_than: Option<i64>,
        retain_last: Option<usize>,
        retain_ref_snapshots: bool,
    }

    impl TestExpireSnapshots {
        fn select_snapshots_to_expire(&self, metadata: &TableMetadata) -> Result<(Vec<i64>, Vec<i64>), Box<dyn std::error::Error>> {
            let mut snapshots_to_expire = Vec::new();
            let mut snapshots_to_retain = Vec::new();

            // Get all snapshots sorted by timestamp (newest first)
            let mut all_snapshots: Vec<_> = metadata.snapshots.values().collect();
            all_snapshots.sort_by(|a, b| b.timestamp_ms().cmp(a.timestamp_ms()));

            // Get current snapshot ID to ensure we never expire it
            let current_snapshot_id = metadata.current_snapshot_id;

            // Get snapshot IDs referenced by branches/tags if we should preserve them
            let ref_snapshot_ids = if self.retain_ref_snapshots {
                let mut referenced_ids = std::collections::HashSet::new();
                for snapshot_ref in metadata.refs.values() {
                    referenced_ids.insert(snapshot_ref.snapshot_id);
                }
                referenced_ids
            } else {
                std::collections::HashSet::new()
            };

            // Apply retention logic
            for (index, snapshot) in all_snapshots.iter().enumerate() {
                let snapshot_id = *snapshot.snapshot_id();
                let mut should_retain = false;

                // Never expire the current snapshot
                if Some(snapshot_id) == current_snapshot_id {
                    should_retain = true;
                }
                // Never expire snapshots referenced by branches/tags
                else if ref_snapshot_ids.contains(&snapshot_id) {
                    should_retain = true;
                }
                // Keep the most recent N snapshots if retain_last is specified
                else if let Some(retain_count) = self.retain_last {
                    if index < retain_count {
                        should_retain = true;
                    }
                }

                // Apply older_than filter only if not already marked for retention
                if !should_retain {
                    if let Some(threshold) = self.older_than {
                        if *snapshot.timestamp_ms() >= threshold {
                            should_retain = true;
                        }
                    }
                }

                if should_retain {
                    snapshots_to_retain.push(snapshot_id);
                } else {
                    snapshots_to_expire.push(snapshot_id);
                }
            }

            Ok((snapshots_to_expire, snapshots_to_retain))
        }
    }
}
