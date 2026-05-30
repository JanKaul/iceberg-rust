//! Java-parity tests for the `SnapshotManager` API.
//!
//! Mirrors `org.apache.iceberg.TestSnapshotManager` (TestBase +
//! @TestTemplate over V1/V2/V3). Java's `table.manageSnapshots()`
//! returns a `ManageSnapshots` builder exposing:
//!   - `createBranch(name, snapshotId)` / `createBranch(name)`
//!     (defaults to current snapshot)
//!   - `createTag(name, snapshotId)` / `createTag(name)`
//!   - `replaceBranch(name, snapshotId)` / `replaceTag(name, snapshotId)`
//!   - `removeBranch(name)` / `removeTag(name)`
//!   - `fastForwardBranch(target, source)`
//!   - `cherrypick(snapshotId)`
//!   - `setMinSnapshotsToKeep(name, n)` / `setMaxSnapshotAgeMs(name, ms)`
//!     / `setMaxRefAgeMs(name, ms)`
//!
//! Rust's transaction surface only exposes the create/replace path via
//! `Transaction::set_snapshot_ref((name, SnapshotReference))`. There
//! is no remove operation, no cherry-pick, no fast-forward, and no
//! "fail if already exists" / "cannot remove main" validation.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use futures::stream;
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::Catalog, object_store::ObjectStoreBuilder,
    table::Table,
};
use iceberg_rust_spec::spec::{
    partition::PartitionSpec,
    schema::Schema,
    snapshot::{SnapshotReference, SnapshotRetention},
    types::{PrimitiveType, StructField, Type},
};
use iceberg_sql_catalog::SqlCatalog;

// --- Shared test fixtures --------------------------------------------------

fn schema() -> Schema {
    Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "data".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap()
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ])
}

fn batch(start_id: i64, n_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + n_rows as i64).collect();
    let data: Vec<Option<String>> = ids.iter().map(|i| Some(format!("row-{i}"))).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap()
}

async fn fresh_table(name: &str) -> Table {
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
            .await
            .unwrap(),
    );
    Table::builder()
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create table")
}

/// Seed a table with one snapshot. Returns the resulting current
/// snapshot id.
async fn seed_one_snapshot(table: &mut Table) -> i64 {
    let files = write_parquet_partitioned(table, stream::iter(vec![Ok(batch(0, 2))]), None)
        .await
        .expect("write seed");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("seed commit");
    table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot")
        .snapshot_id()
        .to_owned()
}

// --- Passing tests ---------------------------------------------------------

/// Java: `testCreateBranch` — creating a named branch pointing at an
/// existing snapshot adds the branch to `table.refs()`.
#[tokio::test]
async fn test_snapshot_manager_create_branch_per_java() {
    let mut table = fresh_table("create_branch").await;
    let snapshot_id = seed_one_snapshot(&mut table).await;

    let branch_name = "feature_branch".to_string();
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create branch");

    let refs = &table.metadata().refs;
    assert!(
        refs.contains_key(&branch_name),
        "table.refs must contain the new branch",
    );
    let new_ref = &refs[&branch_name];
    assert_eq!(
        new_ref.snapshot_id, snapshot_id,
        "branch must point at the supplied snapshot id",
    );
    assert!(
        matches!(new_ref.retention, SnapshotRetention::Branch { .. }),
        "retention must be Branch, got {:?}",
        new_ref.retention,
    );
}

/// Java: `testCreateTag` — creating a tag adds a Tag-retention ref.
#[tokio::test]
async fn test_snapshot_manager_create_tag_per_java() {
    let mut table = fresh_table("create_tag").await;
    let snapshot_id = seed_one_snapshot(&mut table).await;

    let tag_name = "v1.0".to_string();
    table
        .new_transaction(None)
        .set_snapshot_ref((
            tag_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create tag");

    let refs = &table.metadata().refs;
    assert!(refs.contains_key(&tag_name));
    let tag_ref = &refs[&tag_name];
    assert_eq!(tag_ref.snapshot_id, snapshot_id);
    assert!(
        matches!(tag_ref.retention, SnapshotRetention::Tag { .. }),
        "retention must be Tag, got {:?}",
        tag_ref.retention,
    );
}

/// Java: `testReplaceBranch` — updating an existing branch ref to point
/// at a different snapshot.
#[tokio::test]
async fn test_snapshot_manager_replace_branch_per_java() {
    let mut table = fresh_table("replace_branch").await;
    let first_id = seed_one_snapshot(&mut table).await;

    let branch_name = "experimental".to_string();
    // Create the branch pointing at first snapshot.
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id: first_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create branch");

    // Add a second snapshot.
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(100, 2))]), None)
        .await
        .expect("write second");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit second");
    let second_id = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("second snapshot")
        .snapshot_id()
        .to_owned();
    assert_ne!(first_id, second_id);

    // Now replace the branch ref to point at the second snapshot.
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id: second_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("replace branch");

    assert_eq!(
        table.metadata().refs[&branch_name].snapshot_id,
        second_id,
        "branch must now point at the second snapshot",
    );
}

/// Java: `testReplaceTag` — updating a tag to point at a different snapshot.
#[tokio::test]
async fn test_snapshot_manager_replace_tag_per_java() {
    let mut table = fresh_table("replace_tag").await;
    let first_id = seed_one_snapshot(&mut table).await;

    let tag_name = "v1".to_string();
    table
        .new_transaction(None)
        .set_snapshot_ref((
            tag_name.clone(),
            SnapshotReference {
                snapshot_id: first_id,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create tag");

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(100, 2))]), None)
        .await
        .expect("write second");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit second");
    let second_id = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("second snapshot")
        .snapshot_id()
        .to_owned();

    table
        .new_transaction(None)
        .set_snapshot_ref((
            tag_name.clone(),
            SnapshotReference {
                snapshot_id: second_id,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("replace tag");

    assert_eq!(
        table.metadata().refs[&tag_name].snapshot_id,
        second_id,
        "tag must now point at the second snapshot",
    );
}

/// Java: `testUpdatingBranchRetention` — changing min_snapshots_to_keep
/// / max_snapshot_age_ms on an existing branch.
#[tokio::test]
async fn test_snapshot_manager_updating_branch_retention_per_java() {
    let mut table = fresh_table("update_branch_retention").await;
    let snapshot_id = seed_one_snapshot(&mut table).await;

    let branch_name = "retained".to_string();
    // Create with default retention.
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create branch");

    // Update with explicit retention values.
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(5),
                    max_snapshot_age_ms: Some(86_400_000),
                    max_ref_age_ms: Some(604_800_000),
                },
            },
        ))
        .commit()
        .await
        .expect("update retention");

    match &table.metadata().refs[&branch_name].retention {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            max_ref_age_ms,
        } => {
            assert_eq!(*min_snapshots_to_keep, Some(5));
            assert_eq!(*max_snapshot_age_ms, Some(86_400_000));
            assert_eq!(*max_ref_age_ms, Some(604_800_000));
        }
        other => panic!("expected Branch retention, got {other:?}"),
    }
}

/// Java: `testUpdatingTagMaxRefAge` — changing max_ref_age_ms on an
/// existing tag.
#[tokio::test]
async fn test_snapshot_manager_updating_tag_max_ref_age_per_java() {
    let mut table = fresh_table("update_tag_max_ref_age").await;
    let snapshot_id = seed_one_snapshot(&mut table).await;

    let tag_name = "rolling".to_string();
    table
        .new_transaction(None)
        .set_snapshot_ref((
            tag_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create tag");

    table
        .new_transaction(None)
        .set_snapshot_ref((
            tag_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: Some(3_600_000),
                },
            },
        ))
        .commit()
        .await
        .expect("update tag");

    match &table.metadata().refs[&tag_name].retention {
        SnapshotRetention::Tag { max_ref_age_ms } => {
            assert_eq!(*max_ref_age_ms, Some(3_600_000));
        }
        other => panic!("expected Tag retention, got {other:?}"),
    }
}

/// Java: `testUpdatingBranchMaxRefAge` — changing max_ref_age_ms on a
/// branch. (Java exercises this separately from min_snapshots and
/// max_snapshot_age.)
#[tokio::test]
async fn test_snapshot_manager_updating_branch_max_ref_age_per_java() {
    let mut table = fresh_table("update_branch_max_ref_age").await;
    let snapshot_id = seed_one_snapshot(&mut table).await;

    let branch_name = "weekly".to_string();
    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .commit()
        .await
        .expect("create branch");

    table
        .new_transaction(None)
        .set_snapshot_ref((
            branch_name.clone(),
            SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: Some(604_800_000),
                },
            },
        ))
        .commit()
        .await
        .expect("update branch max ref age");

    match &table.metadata().refs[&branch_name].retention {
        SnapshotRetention::Branch { max_ref_age_ms, .. } => {
            assert_eq!(*max_ref_age_ms, Some(604_800_000));
        }
        other => panic!("expected Branch retention, got {other:?}"),
    }
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: no createBranch(name) overload that defaults to current snapshot; Rust requires snapshot_id explicitly"]
async fn test_snapshot_manager_create_branch_without_snapshot_id_per_java() {
    // Java: testCreateBranchWithoutSnapshotId.
}

#[tokio::test]
#[ignore = "feature gap: SnapshotReference.snapshot_id is i64 (not Option); creating a branch on an empty table requires inventing a snapshot id"]
async fn test_snapshot_manager_create_branch_on_empty_table_per_java() {
    // Java: testCreateBranchOnEmptyTable.
}

#[tokio::test]
#[ignore = "feature gap: no 'fails when ref already exists' validation — Rust's set_snapshot_ref silently overwrites"]
async fn test_snapshot_manager_create_branch_on_empty_table_fails_when_ref_already_exists_per_java()
{
    // Java: testCreateBranchOnEmptyTableFailsWhenRefAlreadyExists.
}

#[tokio::test]
#[ignore = "feature gap: same validation gap (non-empty-table variant)"]
async fn test_snapshot_manager_create_branch_fails_when_ref_already_exists_per_java() {
    // Java: testCreateBranchFailsWhenRefAlreadyExists.
}

#[tokio::test]
#[ignore = "feature gap: same — no 'fails when tag exists' validation"]
async fn test_snapshot_manager_create_tag_fails_when_ref_already_exists_per_java() {
    // Java: testCreateTagFailsWhenRefAlreadyExists.
}

#[tokio::test]
#[ignore = "feature gap: no remove_branch / remove_snapshot_ref API on Transaction"]
async fn test_snapshot_manager_remove_branch_per_java() {
    // Java: testRemoveBranch.
}

#[tokio::test]
#[ignore = "feature gap: same — and no 'fails on nonexistent ref' validation"]
async fn test_snapshot_manager_removing_non_existing_branch_fails_per_java() {
    // Java: testRemovingNonExistingBranchFails.
}

#[tokio::test]
#[ignore = "feature gap: same — and no 'cannot remove main' validation"]
async fn test_snapshot_manager_removing_main_branch_fails_per_java() {
    // Java: testRemovingMainBranchFails.
}

#[tokio::test]
#[ignore = "feature gap: same — no remove_tag API"]
async fn test_snapshot_manager_remove_tag_per_java() {
    // Java: testRemoveTag.
}

#[tokio::test]
#[ignore = "feature gap: same — no 'fails on nonexistent tag' validation"]
async fn test_snapshot_manager_removing_non_existing_tag_fails_per_java() {
    // Java: testRemovingNonExistingTagFails.
}

#[tokio::test]
#[ignore = "feature gap: no replaceBranch validation — Rust silently creates the target if it doesn't exist"]
async fn test_snapshot_manager_replace_branch_non_existing_to_branch_fails_per_java() {
    // Java: testReplaceBranchNonExistingToBranchFails.
}

#[tokio::test]
#[ignore = "feature gap: fast-forward operation absent — Java auto-creates missing branches; Rust would silently set_snapshot_ref"]
async fn test_snapshot_manager_fast_forward_branch_non_existing_from_branch_creates_branch_per_java(
) {
    // Java: testFastForwardBranchNonExistingFromBranchCreatesTheBranch.
}

#[tokio::test]
#[ignore = "feature gap: same as above for replaceBranch — Rust has no auto-create semantics tied to a 'from' branch"]
async fn test_snapshot_manager_replace_branch_non_existing_from_branch_creates_branch_per_java() {
    // Java: testReplaceBranchNonExistingFromBranchCreatesTheBranch.
}

#[tokio::test]
#[ignore = "feature gap: same — fastForwardBranch with missing 'to' branch"]
async fn test_snapshot_manager_fast_forward_branch_non_existing_to_fails_per_java() {
    // Java: testFastForwardBranchNonExistingToFails.
}

#[tokio::test]
#[ignore = "feature gap: no fastForwardBranch(target, source) operation"]
async fn test_snapshot_manager_fast_forward_per_java() {
    // Java: testFastForward.
}

#[tokio::test]
#[ignore = "feature gap: same — no ancestor-walk to assert from-is-ancestor-of-to invariant"]
async fn test_snapshot_manager_fast_forward_when_from_is_not_ancestor_fails_per_java() {
    // Java: testFastForwardWhenFromIsNotAncestorFails.
}

#[tokio::test]
#[ignore = "feature gap: no cherry-pick operation"]
async fn test_snapshot_manager_cherry_pick_dynamic_overwrite_per_java() {
    // Java: testCherryPickDynamicOverwrite.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_snapshot_manager_cherry_pick_dynamic_overwrite_without_parent_per_java() {
    // Java: testCherryPickDynamicOverwriteWithoutParent.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_snapshot_manager_cherry_pick_dynamic_overwrite_conflict_per_java() {
    // Java: testCherryPickDynamicOverwriteConflict.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_snapshot_manager_cherry_pick_dynamic_overwrite_delete_conflict_per_java() {
    // Java: testCherryPickDynamicOverwriteDeleteConflict.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_snapshot_manager_cherry_pick_from_branch_per_java() {
    // Java: testCherryPickFromBranch.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_snapshot_manager_cherry_pick_overwrite_per_java() {
    // Java: testCherryPickOverwrite.
}

#[tokio::test]
#[ignore = "feature gap: no validation that setting Branch retention fields on a Tag-typed ref fails"]
async fn test_snapshot_manager_setting_branch_retention_on_tag_fails_per_java() {
    // Java: testSettingBranchRetentionOnTagFails.
}
