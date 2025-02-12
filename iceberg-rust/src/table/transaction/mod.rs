/*!
 * Defines the [Transaction] type that performs multiple [Operation]s with ACID properties.
*/
use std::collections::HashMap;

use iceberg_rust_spec::spec::{manifest::DataFile, schema::Schema, snapshot::SnapshotReference};

use crate::{catalog::commit::CommitTable, error::Error, table::Table};

use self::operation::Operation;

use super::delete_all_table_files;

pub(crate) mod append;
pub(crate) mod operation;

pub(crate) static APPEND_KEY: &str = "append";
pub(crate) static OVERWRITE_KEY: &str = "overwrite";
pub(crate) static ADD_SCHEMA_KEY: &str = "add-schema";
pub(crate) static SET_DEFAULT_SPEC_KEY: &str = "set-default-spec";
pub(crate) static UPDATE_PROPERTIES_KEY: &str = "update-properties";
pub(crate) static SET_SNAPSHOT_REF_KEY: &str = "set-ref";

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
    operations: HashMap<String, Operation>,
    branch: Option<String>,
}

impl<'table> TableTransaction<'table> {
    /// Create a transaction for the given table.
    pub(crate) fn new(table: &'table mut Table, branch: Option<&str>) -> Self {
        TableTransaction {
            table,
            operations: HashMap::new(),
            branch: branch.map(ToString::to_string),
        }
    }
    //AI! Write documentation
    pub fn add_schema(mut self, schema: Schema) -> Self {
        self.operations
            .insert(ADD_SCHEMA_KEY.to_owned(), Operation::AddSchema(schema));
        self
    }
    /// Update the spec of the table
    pub fn set_default_spec(mut self, spec_id: i32) -> Self {
        self.operations.insert(
            SET_DEFAULT_SPEC_KEY.to_owned(),
            Operation::SetDefaultSpec(spec_id),
        );
        self
    }
    /// Quickly append files to the table
    pub fn append(mut self, files: Vec<DataFile>) -> Self {
        self.operations
            .entry(APPEND_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Append {
                    branch: _,
                    files: old,
                    additional_summary: None,
                } = &mut x
                {
                    old.extend_from_slice(&files)
                }
            })
            .or_insert(Operation::Append {
                branch: self.branch.clone(),
                files,
                additional_summary: None,
            });
        self
    }
    /// Quickly append files to the table
    pub fn overwrite(mut self, files: Vec<DataFile>) -> Self {
        self.operations
            .entry(OVERWRITE_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Overwrite {
                    branch: _,
                    files: old,
                    additional_summary: None,
                } = &mut x
                {
                    old.extend_from_slice(&files)
                }
            })
            .or_insert(Operation::Overwrite {
                branch: self.branch.clone(),
                files,
                additional_summary: None,
            });
        self
    }
    /// Quickly append files to the table
    pub fn overwrite_with_lineage(
        mut self,
        files: Vec<DataFile>,
        additional_summary: HashMap<String, String>,
    ) -> Self {
        self.operations
            .entry(OVERWRITE_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Overwrite {
                    branch: _,
                    files: old,
                    additional_summary: old_lineage,
                } = &mut x
                {
                    old.extend_from_slice(&files);
                    *old_lineage = Some(additional_summary.clone());
                }
            })
            .or_insert(Operation::Overwrite {
                branch: self.branch.clone(),
                files,
                additional_summary: Some(additional_summary),
            });
        self
    }
    /// Update the properties of the table
    pub fn update_properties(mut self, entries: Vec<(String, String)>) -> Self {
        self.operations
            .entry(UPDATE_PROPERTIES_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::UpdateProperties(props) = &mut x {
                    props.extend_from_slice(&entries)
                }
            })
            .or_insert(Operation::UpdateProperties(entries));
        self
    }
    /// Set snapshot reference
    pub fn set_snapshot_ref(mut self, entry: (String, SnapshotReference)) -> Self {
        self.operations.insert(
            SET_SNAPSHOT_REF_KEY.to_owned(),
            Operation::SetSnapshotRef(entry),
        );
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.table.catalog();
        let object_store = self.table.object_store();
        let identifier = self.table.identifier.clone();

        // Save old metadata to be able to remove old data after a rewrite operation
        let delete_data = if self.operations.values().any(|x| {
            matches!(
                x,
                Operation::Overwrite {
                    branch: _,
                    files: _,
                    additional_summary: _,
                }
            )
        }) {
            Some(self.table.metadata())
        } else {
            None
        };

        // Execute the table operations
        let (mut requirements, mut updates) = (Vec::new(), Vec::new());
        for operation in self.operations.into_values() {
            let (requirement, update) = operation
                .execute(self.table.metadata(), self.table.object_store())
                .await?;

            if let Some(requirement) = requirement {
                requirements.push(requirement);
            }
            updates.extend(update);
        }

        let new_table = catalog
            .clone()
            .update_table(CommitTable {
                identifier,
                requirements,
                updates,
            })
            .await?;

        if let Some(old_metadata) = delete_data {
            delete_all_table_files(old_metadata, object_store).await?;
        }

        *self.table = new_table;
        Ok(())
    }
}
