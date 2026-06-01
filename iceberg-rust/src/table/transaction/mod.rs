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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -----------------------------------------------------------------------
    // Placeholders for table-operation + snapshot-producer + partition-spec
    // evolution surfaces.
    //
    // Rust's `Transaction` builder composes `append_data` / `append_delete` /
    // `overwrite` / `replace` / `update_properties` / `set_snapshot_ref` /
    // `expire_snapshots` into one `Operation::Update` commit. The upstream
    // surface is much richer: MergeAppend manifest merging policy, FastAppend
    // vs MergeAppend distinction, dynamic partition overwrite,
    // RowDelta with eq+pos deletes, rewrite-files preserving lineage,
    // rewrite-manifests preserving entries, validate-files-exist hook,
    // WAP staged snapshots, snapshot expiration with branch protection,
    // sequence-number assignment, V3 row-lineage assignment, statistics +
    // partition-statistics registration. Each class below pins the upstream
    // contract as one `#[rstest]` with one `#[case]` per upstream @Test method.
    // -----------------------------------------------------------------------

    // -- TestTransaction (24) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[ignore = "no upstream-shape Transaction surface: empty txn, single+multi op, uncommitted-change detection, conflict + bulk cleanup, retry mechanics, no-custom-delete-func, fast-appends-inside-txn, rewrite-manifests appended directly, unknown-commit-state recovery, recommit, commit-properties, concurrent rewrite vs row-delta + overwrite, extend-base-transaction"]
    fn test_transaction_scenarios(#[case] _scenario: usize) {
        unimplemented!("Transaction");
    }

    // -- TestFastAppend (27) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[ignore = "no FastAppend snapshot producer surface (manifest cleanup, refresh-before-apply, refresh-before-commit, failure + retries, snapshot-id inheritance, partition summaries + limit, branch ref enforcement, V3 row-lineage)"]
    fn test_fast_append_scenarios(#[case] _scenario: usize) {
        unimplemented!("FastAppend");
    }

    // -- TestMergeAppend (31) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[ignore = "no MergeAppend snapshot producer surface (manifest combination policy + minMergeCount + min/max merge size, target file size, partition summaries, snapshot-id inheritance, branch refs, V3 row-lineage)"]
    fn test_merge_append_scenarios(#[case] _scenario: usize) {
        unimplemented!("MergeAppend");
    }

    // -- TestOverwrite (12) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[ignore = "no upstream OverwriteFiles surface (overwriteByRowFilter, validateAddedFilesMatchOverwriteFilter, mixed add+delete content-aware op emission, manifest merging)"]
    fn test_overwrite_scenarios(#[case] _scenario: usize) {
        unimplemented!("OverwriteFiles");
    }

    // -- TestOverwriteWithValidation (32) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[ignore = "no validateXxx hooks (validateNoConflictingAppends / validateNoConflictingDeletes / validateAddedFilesMatchOverwriteFilter / validateFromSnapshot)"]
    fn test_overwrite_with_validation_scenarios(#[case] _scenario: usize) {
        unimplemented!("OverwriteWithValidation");
    }

    // -- TestReplacePartitions (26) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[ignore = "no dynamic-partition overwrite surface (per-partition replace; validateAppendOnly / validateNoConflicting* hooks; void-transform handling)"]
    fn test_replace_partitions_scenarios(#[case] _scenario: usize) {
        unimplemented!("ReplacePartitions");
    }

    // -- TestRowDelta (54) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[case(41)]
    #[case(42)]
    #[case(43)]
    #[case(44)]
    #[case(45)]
    #[case(46)]
    #[case(47)]
    #[case(48)]
    #[case(49)]
    #[case(50)]
    #[case(51)]
    #[case(52)]
    #[case(53)]
    #[case(54)]
    #[ignore = "no RowDelta surface (eq + pos deletes, deletion vectors, validation hooks, sequence-number coordination, branch-scoped, V3 row-lineage)"]
    fn test_row_delta_scenarios(#[case] _scenario: usize) {
        unimplemented!("RowDelta");
    }

    // -- TestRewriteFiles (17) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[ignore = "no RewriteFiles surface (preserve snapshot lineage; eq->pos delete migration; sequence-number preservation; remove-all-deletes; 4-arg data+delete overload; validation of nonexistent/already-deleted files)"]
    fn test_rewrite_files_scenarios(#[case] _scenario: usize) {
        unimplemented!("RewriteFiles");
    }

    // -- TestRewriteManifests (30) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[ignore = "no RewriteManifests surface (preserve entries; cluster-by-partition; target-manifest-size; replace by predicate)"]
    fn test_rewrite_manifests_scenarios(#[case] _scenario: usize) {
        unimplemented!("RewriteManifests");
    }

    // -- TestDeleteFiles (14) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[ignore = "no DeleteFiles surface (delete by row filter, case-sensitivity, branch-isolation, validateFilesExist hook, V3 deletion vectors)"]
    fn test_delete_files_scenarios(#[case] _scenario: usize) {
        unimplemented!("DeleteFiles");
    }

    // -- TestSnapshotChanges (3) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[ignore = "no per-snapshot SnapshotChanges accessor (addedDataFiles / removedDataFiles iterables, repeated-call caching)"]
    fn test_snapshot_changes_scenarios(#[case] _scenario: usize) {
        unimplemented!("SnapshotChanges");
    }

    // -- TestSnapshotProducer (6) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no SnapshotProducer composite ops"]
    fn test_snapshot_producer_scenarios(#[case] _scenario: usize) {
        unimplemented!("SnapshotProducer");
    }

    // -- TestSnapshotManager (40) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[ignore = "no ManageSnapshots builder (cherry-pick, fast-forward, remove branch/tag with main-protection, retention setters, replace branch/tag validation)"]
    fn test_snapshot_manager_scenarios(#[case] _scenario: usize) {
        unimplemented!("SnapshotManager");
    }

    // -- TestSnapshotLoading (8) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[ignore = "no lazy snapshot loading; Rust deserialises all snapshots eagerly with TableMetadata"]
    fn test_snapshot_loading_scenarios(#[case] _scenario: usize) {
        unimplemented!("SnapshotLoading");
    }

    // -- TestRemoveSnapshots (64) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[case(41)]
    #[case(42)]
    #[case(43)]
    #[case(44)]
    #[case(45)]
    #[case(46)]
    #[case(47)]
    #[case(48)]
    #[case(49)]
    #[case(50)]
    #[case(51)]
    #[case(52)]
    #[case(53)]
    #[case(54)]
    #[case(55)]
    #[case(56)]
    #[case(57)]
    #[case(58)]
    #[case(59)]
    #[case(60)]
    #[case(61)]
    #[case(62)]
    #[case(63)]
    #[case(64)]
    #[ignore = "no ExpireSnapshots surface (orphan-file detection, branch protection, retention age, per-snapshot age, retainLast, cleanExpiredFiles, executor)"]
    fn test_remove_snapshots_scenarios(#[case] _scenario: usize) {
        unimplemented!("RemoveSnapshots");
    }

    // -- TestManifestCleanup (3) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[ignore = "no ManifestCleanup of dangling manifests on expiration"]
    fn test_manifest_cleanup_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestCleanup");
    }

    // -- TestSequenceNumberForV2Table (10) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[ignore = "no V2 sequence-number assignment on new snapshots, manifest entries, delete files"]
    fn test_sequence_number_for_v2_scenarios(#[case] _scenario: usize) {
        unimplemented!("SequenceNumberForV2Table");
    }

    // -- TestSetStatistics (6) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no TableUpdate::SetStatistics variant; no statistics_files field on TableMetadata"]
    fn test_set_statistics_scenarios(#[case] _scenario: usize) {
        unimplemented!("SetStatistics");
    }

    // -- TestSetPartitionStatistics (4) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[ignore = "no TableUpdate::SetPartitionStatistics variant"]
    fn test_set_partition_statistics_scenarios(#[case] _scenario: usize) {
        unimplemented!("SetPartitionStatistics");
    }

    // -- TestUpdatePartitionSpec (40) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[ignore = "no UpdatePartitionSpec builder (add/remove/rename field; commit produces new spec id; rename + add same name; void transform; bucket/truncate/year/month/day/hour evolution)"]
    fn test_update_partition_spec_scenarios(#[case] _scenario: usize) {
        unimplemented!("UpdatePartitionSpec");
    }

    // -- TestTableUpdatePartitionSpec (9) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[ignore = "no table-level UpdatePartitionSpec commit observation tests"]
    fn test_table_update_partition_spec_scenarios(#[case] _scenario: usize) {
        unimplemented!("TableUpdatePartitionSpec");
    }

    // -- TestWapWorkflow (14) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[ignore = "no Write-Audit-Publish workflow (staged snapshots, publish/abort)"]
    fn test_wap_workflow_scenarios(#[case] _scenario: usize) {
        unimplemented!("WapWorkflow");
    }

    // -- TestRowLineageAssignment (20) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[ignore = "no V3 row-lineage assignment during append/overwrite/rewrite operations"]
    fn test_row_lineage_assignment_scenarios(#[case] _scenario: usize) {
        unimplemented!("RowLineageAssignment");
    }

    // -- TestV1ToV2RowDeltaDelete (4) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[ignore = "no V1->V2 format upgrade path with row-delta delete addition"]
    fn test_v1_to_v2_row_delta_delete_scenarios(#[case] _scenario: usize) {
        unimplemented!("V1ToV2RowDeltaDelete");
    }
}
