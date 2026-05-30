use iceberg_rust_spec::{
    manifest::Content, manifest::DataFile, manifest::ManifestEntry,
    manifest_list::ManifestListEntry,
};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{
    error::Error,
    table::manifest_list::ManifestListReader,
    util::{cmp_with_priority, partition_struct_to_vec, summary_to_rectangle, try_sub, Rectangle},
};

/// Split sets of datafiles depending on their partition_values
#[allow(clippy::type_complexity)]
fn split_datafiles_once(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle,
    names: &[&str],
) -> Result<[(Vec<ManifestEntry>, Rectangle); 2], Error> {
    if let Ordering::Equal = cmp_with_priority(&rect.min, &rect.max)? {
        let mut smaller = files.collect::<Result<Vec<_>, Error>>()?;
        let larger = smaller.split_off(smaller.len() / 2);

        return Ok([
            (smaller, Rectangle::new(SmallVec::new(), SmallVec::new())),
            (larger, Rectangle::new(SmallVec::new(), SmallVec::new())),
        ]);
    }

    let mut smaller = Vec::new();
    let mut larger = Vec::new();
    let mut smaller_rect = None;
    let mut larger_rect = None;

    for manifest_entry in files {
        let manifest_entry = manifest_entry?;
        let position = partition_struct_to_vec(manifest_entry.data_file().partition(), names)?;
        // Compare distance to upper and lower bound. Since you can't compute a "norm" for a multidimensional vector where the dimensions have different datatypes,
        // the dimensions are compared individually and the norm is computed by weighing the earlier columns more than the later.
        if let Ordering::Greater = cmp_with_priority(
            &try_sub(&position, &rect.min)?,
            &try_sub(&rect.max, &position)?,
        )? {
            // if closer to upper bound
            larger.push(manifest_entry);

            if larger_rect.is_none() {
                larger_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(larger_rect) = larger_rect.as_mut() {
                larger_rect.expand_with_node(position);
            }
        } else {
            // if closer to lower bound
            smaller.push(manifest_entry);

            if smaller_rect.is_none() {
                smaller_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(smaller_rect) = smaller_rect.as_mut() {
                smaller_rect.expand_with_node(position);
            }
        }
    }
    Ok([
        (
            smaller,
            smaller_rect.expect("No files selected for the smaller rectangle"),
        ),
        (
            larger,
            larger_rect.expect("No files selected for the smaller rectangle"),
        ),
    ])
}

/// Splits the datafiles *n_split* times to decrease the number of datafiles per maniefst. Returns *2^n_splits* lists of manifest entries.
pub(crate) fn split_datafiles(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle,
    names: &[&str],
    n_split: u32,
) -> Result<Vec<Vec<ManifestEntry>>, Error> {
    let [(smaller, smaller_rect), (larger, larger_rect)] =
        split_datafiles_once(files, rect, names)?;
    if n_split == 1 {
        // Filter out empty splits to avoid creating manifests with no entries
        let mut result = Vec::new();
        if !smaller.is_empty() {
            result.push(smaller);
        }
        if !larger.is_empty() {
            result.push(larger);
        }
        Ok(result)
    } else {
        let mut smaller = split_datafiles(
            smaller.into_iter().map(Ok),
            smaller_rect,
            names,
            n_split - 1,
        )?;
        let mut larger =
            split_datafiles(larger.into_iter().map(Ok), larger_rect, names, n_split - 1)?;

        smaller.append(&mut larger);
        Ok(smaller)
    }
}

pub(crate) struct SelectedManifest {
    pub data_manifest: ManifestListEntry,
    pub delete_manifest: Option<ManifestListEntry>,
    pub file_count_all_entries: usize,
}

/// Select the manifest that yields the smallest bounding rectangle after the
/// bounding rectangle of the new values has been added.
pub(crate) fn select_manifest_partitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    bounding_partition_values: &Rectangle,
) -> Result<SelectedManifest, Error> {
    let mut selected_data_state = None;
    let mut selected_delete_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;

        let mut bounds =
            summary_to_rectangle(manifest.partitions.as_ref().ok_or(Error::NotFound(format!(
                "Partition struct in manifest {}",
                manifest.manifest_path
            )))?)?;

        bounds.expand(bounding_partition_values);

        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        match manifest.content {
            iceberg_rust_spec::manifest_list::Content::Data => {
                let Some((selected_bounds, selected_manifest)) = &selected_data_state else {
                    selected_data_state = Some((bounds, manifest));
                    continue;
                };

                match selected_bounds.cmp_with_priority(&bounds)? {
                    Ordering::Greater => {
                        manifest_list_writer.append_ser(selected_manifest)?;
                        selected_data_state = Some((bounds, manifest));
                        continue;
                    }
                    _ => {
                        manifest_list_writer.append_ser(manifest)?;
                        continue;
                    }
                }
            }
            iceberg_rust_spec::manifest_list::Content::Deletes => {
                let Some((selected_bounds, selected_manifest)) = &selected_delete_state else {
                    selected_delete_state = Some((bounds, manifest));
                    continue;
                };

                match selected_bounds.cmp_with_priority(&bounds)? {
                    Ordering::Greater => {
                        manifest_list_writer.append_ser(selected_manifest)?;
                        selected_delete_state = Some((bounds, manifest));
                        continue;
                    }
                    _ => {
                        manifest_list_writer.append_ser(manifest)?;
                        continue;
                    }
                }
            }
        }
    }
    let (_, data_manifest) =
        selected_data_state.ok_or(Error::NotFound("Manifest for insert".to_owned()))?;

    Ok(SelectedManifest {
        data_manifest,
        delete_manifest: selected_delete_state.map(|(_, x)| x),
        file_count_all_entries,
    })
}

/// Select the manifest with the smallest number of rows.
pub(crate) fn select_manifest_unpartitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
) -> Result<SelectedManifest, Error> {
    let mut selected_data_state = None;
    let mut selected_delete_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;
        // TODO: should this also account for existing_rows_count / existing_files_count?
        let row_count = manifest.added_rows_count;
        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        match manifest.content {
            iceberg_rust_spec::manifest_list::Content::Data => {
                let Some((selected_row_count, selected_manifest)) = &selected_data_state else {
                    selected_data_state = Some((row_count, manifest));
                    continue;
                };

                // If the file doesn't have any rows, we select it
                let Some(row_count) = row_count else {
                    selected_data_state = Some((row_count, manifest));
                    continue;
                };

                if selected_row_count.is_some_and(|x| x > row_count) {
                    manifest_list_writer.append_ser(selected_manifest)?;
                    selected_data_state = Some((Some(row_count), manifest));
                    continue;
                } else {
                    manifest_list_writer.append_ser(manifest)?;
                    continue;
                }
            }
            iceberg_rust_spec::manifest_list::Content::Deletes => {
                let Some((selected_row_count, selected_manifest)) = &selected_delete_state else {
                    selected_delete_state = Some((row_count, manifest));
                    continue;
                };

                // If the file doesn't have any rows, we select it
                let Some(row_count) = row_count else {
                    selected_delete_state = Some((row_count, manifest));
                    continue;
                };

                if selected_row_count.is_some_and(|x| x > row_count) {
                    manifest_list_writer.append_ser(selected_manifest)?;
                    selected_delete_state = Some((Some(row_count), manifest));
                    continue;
                } else {
                    manifest_list_writer.append_ser(manifest)?;
                    continue;
                }
            }
        }
    }
    let (_, data_manifest) =
        selected_data_state.ok_or(Error::NotFound("Manifest for insert".to_owned()))?;

    Ok(SelectedManifest {
        data_manifest,
        delete_manifest: selected_delete_state.map(|(_, x)| x),
        file_count_all_entries,
    })
}

pub(crate) fn append_summary(files: &[DataFile]) -> Option<HashMap<String, String>> {
    if files.is_empty() {
        return None;
    }

    let (mut added_data_files, mut added_records, mut added_files_size) = (0usize, 0i64, 0i64);

    for file in files.iter().filter(|f| *f.content() == Content::Data) {
        added_data_files += 1;
        added_records += file.record_count();
        added_files_size += file.file_size_in_bytes();
    }

    Some(HashMap::from([
        ("added-files-size".into(), added_files_size.to_string()),
        ("added-records".into(), added_records.to_string()),
        ("added-data-files".into(), added_data_files.to_string()),
    ]))
}

// --- TestFastAppend port (cycle I1) ---------------------------------------
//
// Java's `TestFastAppend` (TestBase + @TestTemplate over V1/V2/V3) covers
// the FastAppend snapshot producer end-to-end:
//
//   - `table.newFastAppend().appendFile(f).commit()` writes a fresh
//     append-only manifest per spec; no merging with prior manifests.
//   - Sequence numbers (V2+): start at 0 on an empty table, increment
//     to 1 on the first commit, etc.
//   - Snapshot summary metrics: added-records, added-data-files,
//     added-files-size, total-records, total-data-files, partition
//     summaries (default off, opt-in via property up to a configurable
//     limit).
//   - Manifest cleanup on commit failure; idempotent re-writes on retry;
//     snapshot-id inheritance on appended manifests.
//   - Branch refs: append-to-existing-branch, append-creates-branch-if-
//     needed, append-empty-branch, reject append-to-null-branch + reject
//     append-to-tag.
//   - V3 row lineage: first-row-id assignment from base.next_row_id;
//     added_rows count.
//
// Rust state for FastAppend (`iceberg-rust/src/table/transaction/`):
//   - `Transaction::append_data(files)` exists on the transaction
//     builder (mod.rs line 123) and `commit()` (line 475) applies it.
//   - The summary metric helpers above (this file's append-summary
//     computation) match the canonical Java keys.
//   - Manifest cleanup, retries, branch-ref enforcement, and partition
//     summaries are NOT explicitly tested at the unit level; some are
//     not implemented at all.
//   - V3 row-lineage fields are not on the Snapshot struct (separate
//     gap pinned by cycle H22 in iceberg-rust-spec).
//
// All 27 Java @TestTemplate scenarios are pinned `#[ignore]` here so
// the eventual iceberg-rust append-path implementation has a ready
// spec. Each test body documents the Java method's observable contract.

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "feature gap: no per-spec manifest write path; Java's testAddManyFiles writes 2 * MIN_FILE_GROUP_SIZE data files via newFastAppend and verifies validateTableFiles round-trips them"]
    fn test_fast_append_add_many_files_per_java() {
        // Java: testAddManyFiles. Generate 2 * MIN_FILE_GROUP_SIZE files
        // (partitions alternate 0/1). newFastAppend.appendFile per file.
        // After commit: validateTableFiles confirms every data file is
        // reachable from the new snapshot's manifests.
    }

    #[test]
    #[ignore = "feature gap: spec-evolution + fast-append flow; commits 2 manifests (one per spec) when files come from different specs"]
    fn test_fast_append_empty_table_with_different_specs_per_java() {
        // Java: testEmptyTableFastAppendFilesWithDifferentSpecs.
        // table.updateSpec().addField("id").commit() promotes spec id.
        // newFastAppend.appendFile(FILE_A).appendFile(fileNewSpec).
        // committedSnapshot.allManifests has size 2 (one per spec id);
        // V2: lastSequenceNumber == 1; V1: == 0.
    }

    #[test]
    #[ignore = "feature gap: empty-table append must create the first snapshot from a single data file"]
    fn test_fast_append_empty_table_append_per_java() {
        // Java: testEmptyTableAppend. table.newFastAppend.appendFile(FILE_A).commit()
        // creates first snapshot; lastSequenceNumber bumps to 1 on V2.
    }

    #[test]
    #[ignore = "feature gap: appending a pre-built ManifestFile (rather than DataFiles) on an empty table"]
    fn test_fast_append_empty_table_append_manifest_per_java() {
        // Java: testEmptyTableAppendManifest. newFastAppend.appendManifest(m).commit().
    }

    #[test]
    #[ignore = "feature gap: mixing appendFile + appendManifest in one fast-append; both must be present in the new snapshot"]
    fn test_fast_append_empty_table_append_files_and_manifest_per_java() {
        // Java: testEmptyTableAppendFilesAndManifest.
    }

    #[test]
    #[ignore = "feature gap: appending to a non-empty table appends a fresh manifest (NO merging with existing manifests)"]
    fn test_fast_append_non_empty_table_append_per_java() {
        // Java: testNonEmptyTableAppend. After 1 commit, second commit
        // produces a separate manifest — distinct from MergeAppend.
    }

    #[test]
    #[ignore = "feature gap: fast-append must NOT merge with prior manifests (key behaviour vs MergeAppend)"]
    fn test_fast_append_no_merge_per_java() {
        // Java: testNoMerge.
    }

    #[test]
    #[ignore = "feature gap: re-applying a fast-append after the base metadata changes must re-pick the latest base (not the snapshotted one)"]
    fn test_fast_append_refresh_before_apply_per_java() {
        // Java: testRefreshBeforeApply. Concurrent metadata change before apply().
    }

    #[test]
    #[ignore = "feature gap: refresh-before-commit retries the commit against the latest base metadata"]
    fn test_fast_append_refresh_before_commit_per_java() {
        // Java: testRefreshBeforeCommit.
    }

    #[test]
    #[ignore = "feature gap: commit failure (CommitFailedException) must NOT leave a partial snapshot behind"]
    fn test_fast_append_failure_per_java() {
        // Java: testFailure.
    }

    #[test]
    #[ignore = "feature gap: PROPERTY commit.retry.num-retries controls retry count on commit conflicts; default 4, configurable"]
    fn test_fast_append_increase_num_retries_per_java() {
        // Java: testIncreaseNumRetries.
    }

    #[test]
    #[ignore = "feature gap: appended manifest files belong to the new snapshot's manifest list; cleanup on commit failure deletes them"]
    fn test_fast_append_manifest_cleanup_per_java() {
        // Java: testAppendManifestCleanup.
    }

    #[test]
    #[ignore = "feature gap: write_new_manifests must be idempotent — re-calling apply() on the same SnapshotProducer instance returns the same manifest paths"]
    fn test_fast_append_write_new_manifests_idempotency_per_java() {
        // Java: testWriteNewManifestsIdempotency.
    }

    #[test]
    #[ignore = "feature gap: cleanup of new manifest files when the commit aborts"]
    fn test_fast_append_write_new_manifests_cleanup_per_java() {
        // Java: testWriteNewManifestsCleanup.
    }

    #[test]
    #[ignore = "feature gap: TableProperty SNAPSHOT_ID_INHERITANCE_ENABLED makes appended manifests inherit the snapshot's id rather than carry their own"]
    fn test_fast_append_manifest_with_snapshot_id_inheritance_per_java() {
        // Java: testAppendManifestWithSnapshotIdInheritance.
    }

    #[test]
    #[ignore = "feature gap: commit failure mid-flight with snapshot-id inheritance enabled must still clean up the new manifest"]
    fn test_fast_append_manifest_failure_with_snapshot_id_inheritance_per_java() {
        // Java: testAppendManifestFailureWithSnapshotIdInheritance.
    }

    #[test]
    #[ignore = "feature gap: invalid input manifest (wrong content type, etc.) must be rejected at appendManifest() time"]
    fn test_fast_append_invalid_append_manifest_per_java() {
        // Java: testInvalidAppendManifest.
    }

    #[test]
    #[ignore = "feature gap: unpartitioned table snapshot summary must not include per-partition entries (just the totals)"]
    fn test_fast_append_partition_summaries_on_unpartitioned_table_per_java() {
        // Java: testPartitionSummariesOnUnpartitionedTable.
    }

    #[test]
    #[ignore = "feature gap: by default, partition-level summaries are NOT included in the snapshot summary"]
    fn test_fast_append_default_partition_summaries_per_java() {
        // Java: testDefaultPartitionSummaries.
    }

    #[test]
    #[ignore = "feature gap: TableProperty WRITE_PARTITION_SUMMARY_LIMIT > 0 opts into per-partition summary entries"]
    fn test_fast_append_included_partition_summaries_per_java() {
        // Java: testIncludedPartitionSummaries.
    }

    #[test]
    #[ignore = "feature gap: WRITE_PARTITION_SUMMARY_LIMIT caps how many per-partition entries the summary carries; excess partitions roll into a 'changed' summary"]
    fn test_fast_append_included_partition_summary_limit_per_java() {
        // Java: testIncludedPartitionSummaryLimit.
    }

    #[test]
    #[ignore = "feature gap: appending to an existing named branch updates that branch's current_snapshot_id without touching main"]
    fn test_fast_append_to_existing_branch_per_java() {
        // Java: testAppendToExistingBranch.
    }

    #[test]
    #[ignore = "feature gap: appending to a NEW branch name creates the branch ref on commit"]
    fn test_fast_append_creates_branch_if_needed_per_java() {
        // Java: testAppendCreatesBranchIfNeeded.
    }

    #[test]
    #[ignore = "feature gap: append to a new branch on an empty table — must create both the snapshot and the branch ref"]
    fn test_fast_append_to_branch_empty_table_per_java() {
        // Java: testAppendToBranchEmptyTable.
    }

    #[test]
    #[ignore = "feature gap: append-to-null-branch must throw with a specific error (branch name cannot be null)"]
    fn test_fast_append_to_null_branch_fails_per_java() {
        // Java: testAppendToNullBranchFails.
    }

    #[test]
    #[ignore = "feature gap: append-to-tag must throw (tags are read-only refs); error message must call out the ref kind"]
    fn test_fast_append_to_tag_fails_per_java() {
        // Java: testAppendToTagFails.
    }

    #[test]
    #[ignore = "feature gap: V3 row-lineage — newFastAppend on a V3 table assigns Snapshot.first_row_id from base.next_row_id and bumps next_row_id by added_rows"]
    fn test_fast_append_v3_row_lineage_per_java() {
        // Java has dedicated V3 row-lineage tests in TestRowLineageMetadata
        // (cycle H22) and a few V3-only assertions interleaved in this
        // class. Pin the FastAppend-specific row-lineage contract here.
    }
}
