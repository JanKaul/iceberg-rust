//! Transaction module for atomic table operations
//!
//! This module provides the transaction system for Iceberg tables, allowing multiple
//! operations to be grouped and executed atomically. The main types are:
//!
//! * [`TableTransaction`] - Builder for creating and executing atomic transactions
//! * [`Operation`] - Individual operations that can be part of a transaction
//!
//! Transactions ensure that either all operations succeed or none do, maintaining
//! table consistency. Common operations include:
//!
//! * Adding/updating schemas
//! * Appending data files
//! * Replacing data files
//! * Updating table properties
//! * Managing snapshots and branches

use std::collections::HashMap;
use tracing::{debug, instrument};

use iceberg_rust_spec::spec::{manifest::DataFile, schema::Schema, snapshot::SnapshotReference};

use crate::table::transaction::append::append_summary;
use crate::table::transaction::operation::SequenceGroup;
use crate::{catalog::commit::CommitTable, error::Error, table::Table};

use self::operation::Operation;

pub(crate) mod append;
pub(crate) mod operation;
pub(crate) mod overwrite;

pub(crate) static ADD_SCHEMA_INDEX: usize = 0;
pub(crate) static SET_DEFAULT_SPEC_INDEX: usize = 1;
pub(crate) static APPEND_INDEX: usize = 2;
pub(crate) static APPEND_SEQUENCE_GROUPS_INDEX: usize = 3;
pub(crate) static REPLACE_INDEX: usize = 4;
pub(crate) static OVERWRITE_INDEX: usize = 5;
pub(crate) static UPDATE_PROPERTIES_INDEX: usize = 6;
pub(crate) static SET_SNAPSHOT_REF_INDEX: usize = 7;
pub(crate) static EXPIRE_SNAPSHOTS_INDEX: usize = 8;

pub(crate) static NUM_OPERATIONS: usize = 9;

/// A transaction that can perform multiple operations on a table atomically
///
/// TableTransaction allows grouping multiple table operations (like schema updates,
/// appends, overwrites) into a single atomic transaction. The transaction must be
/// committed for changes to take effect.
///
/// # Type Parameters
/// * `'table` - Lifetime of the reference to the table being modified
///
/// # Examples
/// ```
/// let mut table = // ... get table reference
/// table.new_transaction(None)
///     .add_schema(new_schema)
///     .append(data_files)
///     .commit()
///     .await?;
/// ```
pub struct TableTransaction<'table> {
    table: &'table mut Table,
    operations: Vec<Option<Operation>>,
    branch: Option<String>,
}

impl<'table> TableTransaction<'table> {
    /// Create a transaction for the given table.
    pub(crate) fn new(table: &'table mut Table, branch: Option<&str>) -> Self {
        TableTransaction {
            table,
            operations: (0..NUM_OPERATIONS).map(|_| None).collect(), // 6 operation types
            branch: branch.map(ToString::to_string),
        }
    }
    /// Adds a new schema to the table
    ///
    /// This operation adds a new schema version to the table. The schema ID will be
    /// automatically assigned when the transaction is committed.
    ///
    /// # Arguments
    /// * `schema` - The new schema to add to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    pub fn add_schema(mut self, schema: Schema) -> Self {
        self.operations[ADD_SCHEMA_INDEX] = Some(Operation::AddSchema(schema));
        self
    }
    /// Sets the default partition specification ID for the table
    ///
    /// # Arguments
    /// * `spec_id` - The ID of the partition specification to set as default
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// The specified partition specification must already exist in the table metadata.
    pub fn set_default_spec(mut self, spec_id: i32) -> Self {
        self.operations[SET_DEFAULT_SPEC_INDEX] = Some(Operation::SetDefaultSpec(spec_id));
        self
    }
    /// Appends new data files to the table
    ///
    /// This operation adds new data files to the table's current snapshot. Multiple
    /// append operations in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `files` - Vector of data files to append to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .append_data(data_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn append_data(mut self, files: Vec<DataFile>) -> Self {
        if self.operations[APPEND_SEQUENCE_GROUPS_INDEX].is_some() {
            panic!("Cannot use append and append_sequence_group in the same transaction");
        }
        let summary = append_summary(&files);

        if let Some(ref mut operation) = self.operations[APPEND_INDEX] {
            if let Operation::Append {
                data_files: old, ..
            } = operation
            {
                old.extend_from_slice(&files);
            }
        } else {
            self.operations[APPEND_INDEX] = Some(Operation::Append {
                branch: self.branch.clone(),
                data_files: files,
                delete_files: Vec::new(),
                additional_summary: summary,
            });
        }
        self
    }
    /// Appends delete files to the table
    ///
    /// This operation adds files that mark records for deletion in the table's current snapshot.
    /// Multiple delete operations in the same transaction will be combined. The delete files
    /// specify which records should be removed when reading the table.
    ///
    /// # Arguments
    /// * `files` - Vector of delete files to append to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .append_delete(delete_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn append_delete(mut self, files: Vec<DataFile>) -> Self {
        if self.operations[APPEND_SEQUENCE_GROUPS_INDEX].is_some() {
            panic!("Cannot use append and append_sequence_group in the same transaction");
        }
        if let Some(ref mut operation) = self.operations[APPEND_INDEX] {
            if let Operation::Append {
                delete_files: old, ..
            } = operation
            {
                old.extend_from_slice(&files);
            }
        } else {
            self.operations[APPEND_INDEX] = Some(Operation::Append {
                branch: self.branch.clone(),
                data_files: Vec::new(),
                delete_files: files,
                additional_summary: None,
            });
        }
        self
    }

    /// Appends a group of data and delete files to the table
    ///
    pub fn append_sequence_group(
        mut self,
        data_files: Vec<DataFile>,
        delete_files: Vec<DataFile>,
    ) -> Self {
        if self.operations[APPEND_INDEX].is_some() {
            panic!("Cannot use append and append_sequence_group in the same transaction");
        }
        if let Some(ref mut operation) = self.operations[APPEND_SEQUENCE_GROUPS_INDEX] {
            if let Operation::AppendSequenceGroups {
                sequence_groups: old,
                ..
            } = operation
            {
                old.push(SequenceGroup {
                    delete_files,
                    data_files,
                });
            }
        } else {
            self.operations[APPEND_SEQUENCE_GROUPS_INDEX] = Some(Operation::AppendSequenceGroups {
                branch: self.branch.clone(),
                sequence_groups: vec![SequenceGroup {
                    delete_files,
                    data_files,
                }],
            });
        }
        self
    }
    /// Overwrites specific data files in the table with new ones
    ///
    /// This operation replaces specified existing data files with new ones, rather than
    /// replacing all files (like `replace`) or adding new files (like `append`). It allows
    /// for selective replacement of data files based on the mapping provided.
    ///
    /// Multiple overwrite operations in the same transaction will be combined, with new
    /// data files appended and the files-to-overwrite mapping merged.
    ///
    /// # Arguments
    /// * `files` - Vector of new data files to add to the table
    /// * `files_to_overwrite` - HashMap mapping manifest file paths to lists of data file
    ///   paths that should be overwritten/replaced
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    ///
    /// let mut files_to_overwrite = HashMap::new();
    /// files_to_overwrite.insert(
    ///     "manifest-001.avro".to_string(),
    ///     vec!["data-001.parquet".to_string(), "data-002.parquet".to_string()]
    /// );
    ///
    /// let transaction = table.new_transaction(None)
    ///     .overwrite(new_data_files, files_to_overwrite)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn overwrite(
        mut self,
        files: Vec<DataFile>,
        files_to_overwrite: HashMap<String, Vec<String>>,
    ) -> Self {
        let summary = append_summary(&files);

        if let Some(ref mut operation) = self.operations[OVERWRITE_INDEX] {
            if let Operation::Overwrite {
                data_files: old_data_files,
                files_to_overwrite: old_files_to_overwrite,
                ..
            } = operation
            {
                old_data_files.extend_from_slice(&files);
                old_files_to_overwrite.extend(files_to_overwrite);
            }
        } else {
            self.operations[OVERWRITE_INDEX] = Some(Operation::Overwrite {
                branch: self.branch.clone(),
                data_files: files,
                files_to_overwrite,
                additional_summary: summary,
            });
        }
        self
    }
    /// Replaces all data files in the table with new ones
    ///
    /// This operation removes all existing data files and replaces them with the provided
    /// files. Multiple replace operations in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `files` - Vector of data files that will replace the existing ones
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .replace(new_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn replace(mut self, files: Vec<DataFile>) -> Self {
        if let Some(ref mut operation) = self.operations[REPLACE_INDEX] {
            if let Operation::Replace {
                branch: _,
                files: old,
                additional_summary: None,
            } = operation
            {
                old.extend_from_slice(&files);
            }
        } else {
            self.operations[REPLACE_INDEX] = Some(Operation::Replace {
                branch: self.branch.clone(),
                files,
                additional_summary: None,
            });
        }
        self
    }
    /// Quickly append files to the table
    pub fn replace_with_lineage(
        mut self,
        files: Vec<DataFile>,
        additional_summary: std::collections::HashMap<String, String>,
    ) -> Self {
        if let Some(ref mut operation) = self.operations[REPLACE_INDEX] {
            if let Operation::Replace {
                branch: _,
                files: old,
                additional_summary: old_lineage,
            } = operation
            {
                old.extend_from_slice(&files);
                *old_lineage = Some(additional_summary.clone());
            }
        } else {
            self.operations[REPLACE_INDEX] = Some(Operation::Replace {
                branch: self.branch.clone(),
                files,
                additional_summary: Some(additional_summary),
            });
        }
        self
    }
    /// Updates the table properties with new key-value pairs
    ///
    /// This operation adds or updates table properties. Multiple update operations
    /// in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `entries` - Vector of (key, value) pairs to update in the table properties
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .update_properties(vec![
    ///         ("write.format.default".to_string(), "parquet".to_string()),
    ///         ("write.metadata.compression-codec".to_string(), "gzip".to_string())
    ///     ])
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn update_properties(mut self, entries: Vec<(String, String)>) -> Self {
        if let Some(ref mut operation) = self.operations[UPDATE_PROPERTIES_INDEX] {
            if let Operation::UpdateProperties(props) = operation {
                props.extend_from_slice(&entries);
            }
        } else {
            self.operations[UPDATE_PROPERTIES_INDEX] = Some(Operation::UpdateProperties(entries));
        }
        self
    }
    /// Sets a snapshot reference for the table
    ///
    /// This operation creates or updates a named reference to a specific snapshot,
    /// allowing for features like branches and tags.
    ///
    /// # Arguments
    /// * `entry` - Tuple of (reference name, snapshot reference) defining the reference
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .set_snapshot_ref((
    ///         "test-branch".to_string(),
    ///         SnapshotReference {
    ///             snapshot_id: 123,
    ///             retention: SnapshotRetention::default(),
    ///         }
    ///     ))
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn set_snapshot_ref(mut self, entry: (String, SnapshotReference)) -> Self {
        self.operations[SET_SNAPSHOT_REF_INDEX] = Some(Operation::SetSnapshotRef(entry));
        self
    }

    /// Expire snapshots based on the provided configuration
    ///
    /// This operation expires snapshots according to the retention policies specified.
    /// It can expire snapshots older than a certain timestamp, retain only the most recent N snapshots,
    /// and optionally clean up orphaned data files.
    ///
    /// # Arguments
    /// * `older_than` - Optional timestamp (ms since Unix epoch) to expire snapshots older than this time
    /// * `retain_last` - Optional number of most recent snapshots to keep, regardless of timestamp
    /// * `clean_orphan_files` - Whether to clean up data files that are no longer referenced
    /// * `retain_ref_snapshots` - Whether to preserve snapshots that are referenced by branches/tags
    /// * `dry_run` - Whether to perform a dry run without actually deleting anything
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let result = table.new_transaction(None)
    ///     .expire_snapshots(
    ///         Some(chrono::Utc::now().timestamp_millis() - 7 * 24 * 60 * 60 * 1000),
    ///         Some(5),
    ///         true,
    ///         true,
    ///         false
    ///     )
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn expire_snapshots(
        mut self,
        older_than: Option<i64>,
        retain_last: Option<usize>,
        clean_orphan_files: bool,
        retain_ref_snapshots: bool,
        dry_run: bool,
    ) -> Self {
        self.operations[EXPIRE_SNAPSHOTS_INDEX] = Some(Operation::ExpireSnapshots {
            older_than,
            retain_last,
            _clean_orphan_files: clean_orphan_files,
            retain_ref_snapshots,
            dry_run,
        });
        self
    }

    /// Commits all operations in this transaction atomically
    ///
    /// This method executes all operations in the transaction and updates the table
    /// metadata. The changes are atomic - either all operations succeed or none do.
    /// After commit, the transaction is consumed and the table is updated with the
    /// new metadata.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok(()) if the commit succeeds, Error if it fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * Any operation fails to execute
    /// * The catalog update fails
    /// * Cleanup of old data files fails (for replace operations)
    ///
    /// # Examples
    /// ```
    /// let result = table.new_transaction(None)
    ///     .append(data_files)
    ///     .update_properties(properties)
    ///     .commit()
    ///     .await?;
    /// ```
    #[instrument(name = "iceberg_rust::table::transaction::commit", level = "debug", skip(self), fields(
        table_identifier = %self.table.identifier,
        branch = ?self.branch
    ))]
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.table.catalog();
        let identifier = self.table.identifier.clone();

        // Execute the table operations
        let (mut requirements, mut updates) = (Vec::new(), Vec::new());
        for operation in self.operations.into_iter().flatten() {
            let (requirement, update) = operation
                .execute(self.table.metadata(), self.table.object_store())
                .await?;

            if let Some(requirement) = requirement {
                requirements.push(requirement);
            }
            updates.extend(update);
        }

        if updates.is_empty() {
            return Ok(());
        }

        debug!(
            "Committing {} updates to table {}: requirements={:?}, updates={:?}",
            updates.len(),
            identifier,
            requirements,
            updates
        );

        let new_table = catalog
            .clone()
            .update_table(CommitTable {
                identifier,
                requirements,
                updates,
            })
            .await?;

        *self.table = new_table;
        Ok(())
    }
}

// --- TestTransaction port (cycle I2) --------------------------------------
//
// Java's `TestTransaction` (TestBase + @TestTemplate over V1/V2/V3)
// covers the multi-op transaction flow: `table.newTransaction()` returns
// a Transaction builder; `tx.newAppend()` / `tx.newDelete()` / etc.
// stage operations against a Transaction-local view of the metadata;
// `tx.commitTransaction()` applies the full chain to the underlying
// table in one atomic commit.
//
// Java pins behaviour around:
//   - Empty transaction (no-op commit).
//   - Single-op + multi-op transactions.
//   - Detecting uncommitted updates between ops (`Cannot commit:
//     transaction is dirty after rollback`).
//   - Transaction-vs-table conflict resolution (retry on optimistic-
//     lock failure; bulk-deletion cleanup on giving up).
//   - Configurable retry count (`commit.retry.num-retries`).
//   - Merge-append + schema-update + manifest-rewrite paths inside a
//     transaction.
//   - Snapshot-id inheritance opt-in for transaction-appended manifests.
//   - "Unknown commit state" handling that must NOT delete metadata.
//   - Commit-property propagation.
//   - Concurrent manifest rewrites against pending RowDelta / Overwrite.
//   - `extend(baseTransaction)` to chain a child transaction on top.
//
// Rust state: a `Transaction` builder exists with `append_data /
// append_delete / overwrite / replace / replace_with_lineage /
// update_properties / set_snapshot_ref / expire_snapshots / commit`
// (this file). It composes multiple ops into a single
// `Operation::Update` commit (mod.rs::475). Behaviour around retry
// loops, dirty-state detection, conflict resolution, snapshot-id
// inheritance, unknown-commit-state cleanup, and `extend(baseTxn)`
// is either not implemented or not unit-tested.
//
// All 24 Java @TestTemplate scenarios pinned `#[ignore]` so the
// eventual transaction work has a ready spec.

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "feature gap: tx.commitTransaction() on an empty transaction must be a successful no-op"]
    fn test_transaction_empty_transaction_per_java() {
        // Java: testEmptyTransaction.
    }

    #[test]
    #[ignore = "feature gap: single newAppend inside a transaction; commit must produce one snapshot"]
    fn test_transaction_single_operation_transaction_per_java() {
        // Java: testSingleOperationTransaction.
    }

    #[test]
    #[ignore = "feature gap: chained newAppend + newDelete + newOverwrite in one transaction; commit produces a SINGLE snapshot summarising all changes"]
    fn test_transaction_multiple_operation_transaction_per_java() {
        // Java: testMultipleOperationTransaction.
    }

    #[test]
    #[ignore = "feature gap: tx.newAppend() borrows the live table's metadata; multiple op builders share the same transaction-local view"]
    fn test_transaction_multiple_operation_transaction_from_table_per_java() {
        // Java: testMultipleOperationTransactionFromTable.
    }

    #[test]
    #[ignore = "feature gap: committing an op while a sibling op is still uncommitted must throw — only one pending op at a time"]
    fn test_transaction_detects_uncommitted_change_per_java() {
        // Java: testDetectsUncommittedChange.
    }

    #[test]
    #[ignore = "feature gap: same detection but at commitTransaction() time — must throw if any sibling op is uncommitted"]
    fn test_transaction_detects_uncommitted_change_on_commit_per_java() {
        // Java: testDetectsUncommittedChangeOnCommit.
    }

    #[test]
    #[ignore = "feature gap: when the underlying table changes during a transaction, commit must detect the conflict and either retry or fail"]
    fn test_transaction_conflict_per_java() {
        // Java: testTransactionConflict.
    }

    #[test]
    #[ignore = "feature gap: on transaction commit failure, all manifests/data files the transaction wrote must be bulk-deleted from object_store"]
    fn test_transaction_failure_bulk_deletion_cleanup_per_java() {
        // Java: testTransactionFailureBulkDeletionCleanup.
    }

    #[test]
    #[ignore = "feature gap: optimistic-lock conflict triggers automatic retry up to commit.retry.num-retries (default 4)"]
    fn test_transaction_retry_per_java() {
        // Java: testTransactionRetry.
    }

    #[test]
    #[ignore = "feature gap: retry inside a MergeAppend transaction must reattempt the merge against the latest base manifests"]
    fn test_transaction_retry_merge_append_per_java() {
        // Java: testTransactionRetryMergeAppend.
    }

    #[test]
    #[ignore = "feature gap: multi-op transaction retry must NOT leave intermediate manifests behind on each failed attempt; only the final successful manifest survives"]
    fn test_transaction_multiple_update_retry_merge_cleanup_per_java() {
        // Java: testMultipleUpdateTransactionRetryMergeCleanup.
    }

    #[test]
    #[ignore = "feature gap: schema-update inside a retried transaction must re-apply against the latest schema"]
    fn test_transaction_retry_schema_update_per_java() {
        // Java: testTransactionRetrySchemaUpdate.
    }

    #[test]
    #[ignore = "feature gap: merge-cleanup variant — intermediate merge manifests deleted between retry attempts"]
    fn test_transaction_retry_merge_cleanup_per_java() {
        // Java: testTransactionRetryMergeCleanup.
    }

    #[test]
    #[ignore = "feature gap: retry path for transaction.appendManifests() with snapshot-id-inheritance DISABLED (default V1 path)"]
    fn test_transaction_retry_and_append_manifests_without_snapshot_id_inheritance_per_java() {
        // Java: testTransactionRetryAndAppendManifestsWithoutSnapshotIdInheritance.
    }

    #[test]
    #[ignore = "feature gap: same retry path with snapshot-id-inheritance ENABLED (default V2+ path)"]
    fn test_transaction_retry_and_append_manifests_with_snapshot_id_inheritance_per_java() {
        // Java: testTransactionRetryAndAppendManifestsWithSnapshotIdInheritance.
    }

    #[test]
    #[ignore = "feature gap: transaction cleanup falls back to standard delete (no custom function); pins the default delete path"]
    fn test_transaction_no_custom_delete_func_per_java() {
        // Java: testTransactionNoCustomDeleteFunc.
    }

    #[test]
    #[ignore = "feature gap: tx.newFastAppend() composes with other ops; FastAppend semantics preserved"]
    fn test_transaction_fast_appends_per_java() {
        // Java: testTransactionFastAppends.
    }

    #[test]
    #[ignore = "feature gap: tx.rewriteManifests().appendManifest(...) — manifest appended directly to the rewrite must show up in the new snapshot"]
    fn test_transaction_rewrite_manifests_appended_directly_per_java() {
        // Java: testTransactionRewriteManifestsAppendedDirectly.
    }

    #[test]
    #[ignore = "feature gap: 'unknown commit state' (network failure after request sent) must NOT delete metadata files since the commit MIGHT have succeeded server-side"]
    fn test_transaction_simple_not_deleting_metadata_on_unknown_state_per_java() {
        // Java: testSimpleTransactionNotDeletingMetadataOnUnknownSate.
    }

    #[test]
    #[ignore = "feature gap: tx.commitTransaction() must support re-commit after a transient failure — same transaction state, same outcome"]
    fn test_transaction_recommit_per_java() {
        // Java: testTransactionRecommit.
    }

    #[test]
    #[ignore = "feature gap: tx.commit() must propagate WriteOperation properties (e.g. 'snapshot-property.foo=bar') onto the produced snapshot's summary"]
    fn test_transaction_commit_properties_per_java() {
        // Java: testCommitProperties.
    }

    #[test]
    #[ignore = "feature gap: row-delta inside a transaction must detect a concurrent rewriteManifests() and retry against the rewritten base"]
    fn test_transaction_row_delta_with_concurrent_manifest_rewrite_per_java() {
        // Java: testRowDeltaWithConcurrentManifestRewrite.
    }

    #[test]
    #[ignore = "feature gap: same concurrent-rewrite handling for an overwrite inside a transaction"]
    fn test_transaction_overwrite_with_concurrent_manifest_rewrite_per_java() {
        // Java: testOverwriteWithConcurrentManifestRewrite.
    }

    #[test]
    #[ignore = "feature gap: Transaction.extend(baseTransaction) chains a child txn on top of a still-open parent; commit applies both atomically"]
    fn test_transaction_extend_base_transaction_per_java() {
        // Java: testExtendBaseTransaction.
    }
}
