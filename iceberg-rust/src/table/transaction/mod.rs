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

use iceberg_rust_spec::spec::{manifest::DataFile, schema::Schema, snapshot::SnapshotReference};

use crate::table::transaction::append::append_summary;
pub use crate::table::transaction::operation::DsnGroup;
use crate::{catalog::commit::CommitTable, error::Error, table::Table};

use self::operation::Operation;

pub(crate) mod append;
pub(crate) mod operation;
pub(crate) mod overwrite;

pub(crate) static ADD_SCHEMA_INDEX: usize = 0;
pub(crate) static SET_DEFAULT_SPEC_INDEX: usize = 1;
pub(crate) static APPEND_INDEX: usize = 2;
pub(crate) static REPLACE_INDEX: usize = 3;
pub(crate) static OVERWRITE_INDEX: usize = 4;
pub(crate) static UPDATE_PROPERTIES_INDEX: usize = 5;
pub(crate) static SET_SNAPSHOT_REF_INDEX: usize = 6;

pub(crate) static NUM_OPERATIONS: usize = 7;

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
        let summary = append_summary(&files);

        if let Some(ref mut operation) = self.operations[APPEND_INDEX] {
            if let Operation::Append { dsn_groups, .. } = operation {
                match dsn_groups.last_mut() {
                    Some(g) => g.data_files.extend_from_slice(&files),
                    None => dsn_groups.push(DsnGroup {
                        data_files: files,
                        delete_files: vec![],
                    }),
                };
            } else {
                panic!("Operation at APPEND_INDEX should be an Append");
            }
        } else {
            self.operations[APPEND_INDEX] = Some(Operation::Append {
                branch: self.branch.clone(),
                dsn_groups: vec![DsnGroup {
                    data_files: files,
                    delete_files: vec![],
                }],
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
        if let Some(ref mut operation) = self.operations[APPEND_INDEX] {
            if let Operation::Append {
                branch: _,
                dsn_groups,
                ..
            } = operation
            {
                match dsn_groups.last_mut() {
                    Some(g) => g.delete_files.extend_from_slice(&files),
                    None => dsn_groups.push(DsnGroup {
                        data_files: vec![],
                        delete_files: files,
                    }),
                };
            } else {
                panic!("Operation at APPEND_INDEX should be an Append");
            }
        } else {
            self.operations[APPEND_INDEX] = Some(Operation::Append {
                branch: self.branch.clone(),
                dsn_groups: vec![DsnGroup {
                    data_files: vec![],
                    delete_files: files,
                }],
                additional_summary: None,
            });
        }
        self
    }
    /// Create a new data sequence number for subsequent appends
    pub fn new_data_sequence_number(mut self) -> Self {
        if let Some(Operation::Append {
            branch: _,
            ref mut dsn_groups,
            ..
        }) = self.operations[APPEND_INDEX]
        {
            dsn_groups.push(DsnGroup {
                data_files: vec![],
                delete_files: vec![],
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
