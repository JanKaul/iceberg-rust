//! Java-parity tests for `RewriteFiles`.
//!
//! Mirrors `org.apache.iceberg.TestRewriteFiles` (TestBase +
//! @TestTemplate over V2/V3 × main/branch). Java's
//! `table.newRewrite().rewriteFiles(filesToDelete, newFiles)` atomically
//! replaces a set of existing data files with a set of new ones,
//! preserving snapshot lineage. Optional 4-arg overload also rewrites
//! delete files.
//!
//! Java pins:
//!   - Empty/invalid input rejection ('Missing required files to delete').
//!   - Add-only / delete-only rejection.
//!   - Rewrites preserve snapshot lineage.
//!   - Old-sequence-number assignment for the rewritten manifests.
//!   - Commit-failure cleanup and recovery.
//!   - Replacing equality deletes with position deletes.
//!   - V3 deletion-vector (DV) required-fields validation.
//!
//! Rust state: no `Transaction::rewrite_files` API; the closest path is
//! `Transaction::overwrite(new, files_to_overwrite)` which has its own
//! bugs (cycles I2 + I4). The `rewriteFiles` semantics — atomic
//! N-files-in-place-of-M-files preserving lineage and sequence numbers
//! — are not implemented.

#[tokio::test]
#[ignore = "feature gap: no Transaction::rewrite_files API; Java validates 'Missing required files to delete' on empty input"]
async fn test_rewrite_files_empty_table_per_java() {
    // Java: testEmptyTable. table.newRewrite.rewriteFiles({FILE_A}, {FILE_B})
    // on an empty table throws ValidationException because FILE_A
    // isn't present. Same error when rewriting delete files with a
    // missing source.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles(filesToDelete=ø, newFiles=non-empty) must throw — pure-add is not a rewrite"]
async fn test_rewrite_files_add_only_per_java() {
    // Java: testAddOnly.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles(filesToDelete=non-empty, newFiles=ø) must throw — pure-delete is not a rewrite"]
async fn test_rewrite_files_delete_only_per_java() {
    // Java: testDeleteOnly.
}

#[tokio::test]
#[ignore = "feature gap: same as delete-only — but the input manifest has duplicate entries for the same file"]
async fn test_rewrite_files_delete_with_duplicate_entries_in_manifest_per_java() {
    // Java: testDeleteWithDuplicateEntriesInManifest.
}

#[tokio::test]
#[ignore = "feature gap: standard rewrite — N files in, M files out, snapshot lineage preserved"]
async fn test_rewrite_files_add_and_delete_per_java() {
    // Java: testAddAndDelete. The most-common path:
    // newRewrite.rewriteFiles({FILE_A}, {FILE_NEW}).commit() produces
    // a snapshot whose summary records DELETED (FILE_A) + ADDED (FILE_NEW).
}

#[tokio::test]
#[ignore = "feature gap: 4-arg overload rewriteFiles(dataDelete, dataAdd, deleteDelete, deleteAdd) handles both data files and delete files in one op"]
async fn test_rewrite_files_rewrite_data_and_delete_files_per_java() {
    // Java: testRewriteDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles(..., sequenceNumber) overload assigns the OLD sequence number to the rewritten data so downstream sequence-number tracking stays consistent"]
async fn test_rewrite_files_rewrite_data_and_assign_old_sequence_number_per_java() {
    // Java: testRewriteDataAndAssignOldSequenceNumber.
}

#[tokio::test]
#[ignore = "feature gap: commit failure (CommitFailedException) must NOT leave a partial snapshot behind"]
async fn test_rewrite_files_failure_per_java() {
    // Java: testFailure.
}

#[tokio::test]
#[ignore = "feature gap: same failure path with both data + delete-file rewrites"]
async fn test_rewrite_files_failure_when_rewrite_both_data_and_delete_files_per_java() {
    // Java: testFailureWhenRewriteBothDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: recover-after-failure path"]
async fn test_rewrite_files_recovery_per_java() {
    // Java: testRecovery.
}

#[tokio::test]
#[ignore = "feature gap: same with data + delete-file rewrite"]
async fn test_rewrite_files_recover_when_rewrite_both_data_and_delete_files_per_java() {
    // Java: testRecoverWhenRewriteBothDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: equality-delete -> position-delete migration (a v3-era flow)"]
async fn test_rewrite_files_replace_equality_deletes_with_position_deletes_per_java() {
    // Java: testReplaceEqualityDeletesWithPositionDeletes.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles with deleteFilesToDelete=ALL must clear the delete-file set on the resulting snapshot"]
async fn test_rewrite_files_remove_all_deletes_per_java() {
    // Java: testRemoveAllDeletes.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles must reject inputs referencing a data file that isn't in the current manifest"]
async fn test_rewrite_files_delete_non_existent_file_per_java() {
    // Java: testDeleteNonExistentFile.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles must reject inputs referencing a file that's ALREADY been deleted in a prior snapshot"]
async fn test_rewrite_files_already_deleted_file_per_java() {
    // Java: testAlreadyDeletedFile.
}

#[tokio::test]
#[ignore = "feature gap: rewriting WITH a brand-new delete file added in the same commit"]
async fn test_rewrite_files_new_delete_file_per_java() {
    // Java: testNewDeleteFile.
}
