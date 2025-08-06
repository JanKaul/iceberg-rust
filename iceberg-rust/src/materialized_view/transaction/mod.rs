/*!
 * Defines the [Transaction] type for materialized views to perform multiple [Operation]s with ACID guarantees.
*/

use std::collections::HashMap;

use iceberg_rust_spec::{
    materialized_view_metadata::{RefreshState, REFRESH_STATE},
    spec::{manifest::DataFile, types::StructType, view_metadata::ViewRepresentation},
};

use crate::{
    catalog::commit::{CommitTable, CommitView},
    error::Error,
    table::{
        delete_all_table_files,
        transaction::{
            operation::{DsnGroup, Operation as TableOperation},
            APPEND_INDEX, REPLACE_INDEX,
        },
    },
    view::transaction::operation::Operation as ViewOperation,
};

use super::MaterializedView;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    materialized_view: &'view mut MaterializedView,
    view_operations: Vec<ViewOperation>,
    storage_table_operations: Vec<Option<TableOperation>>,
    branch: Option<String>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut MaterializedView, branch: Option<&str>) -> Self {
        Transaction {
            materialized_view: view,
            view_operations: vec![],
            storage_table_operations: (0..6).map(|_| None).collect(), // 6 operation types
            branch: branch.map(ToString::to_string),
        }
    }

    /// Update the schmema of the view
    pub fn update_representations(
        mut self,
        representations: Vec<ViewRepresentation>,
        schema: StructType,
    ) -> Self {
        self.view_operations
            .push(ViewOperation::UpdateRepresentations {
                representations,
                schema,
                branch: self.branch.clone(),
            });
        self
    }

    /// Update view properties
    pub fn update_properties(mut self, entries: Vec<(String, String)>) -> Self {
        self.view_operations
            .push(ViewOperation::UpdateProperties(entries));
        self
    }

    /// Perform full refresh operation
    pub fn full_refresh(
        mut self,
        files: Vec<DataFile>,
        refresh_state: RefreshState,
    ) -> Result<Self, Error> {
        let refresh_state = serde_json::to_string(&refresh_state)?;
        if let Some(ref mut operation) = self.storage_table_operations[REPLACE_INDEX] {
            if let TableOperation::Replace {
                branch: _,
                files: old,
                additional_summary: old_lineage,
            } = operation
            {
                old.extend_from_slice(&files);
                *old_lineage = Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state.clone(),
                )]));
            }
        } else {
            self.storage_table_operations[REPLACE_INDEX] = Some(TableOperation::Replace {
                branch: self.branch.clone(),
                files,
                additional_summary: Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state,
                )])),
            });
        }
        Ok(self)
    }

    /// Append files to the storage table
    pub fn append(
        mut self,
        files: Vec<DataFile>,
        refresh_state: RefreshState,
    ) -> Result<Self, Error> {
        let refresh_state = serde_json::to_string(&refresh_state)?;
        if let Some(ref mut operation) = self.storage_table_operations[APPEND_INDEX] {
            if let TableOperation::Append {
                branch: _,
                dsn_groups,
                additional_summary: old_lineage,
            } = operation
            {
                match dsn_groups.last_mut() {
                    Some(g) => g.data_files.extend_from_slice(&files),
                    None => dsn_groups.push(DsnGroup {
                        data_files: files,
                        delete_files: vec![],
                    }),
                };
                *old_lineage = Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state.clone(),
                )]));
            }
        } else {
            self.storage_table_operations[APPEND_INDEX] = Some(TableOperation::Append {
                branch: self.branch.clone(),
                dsn_groups: vec![DsnGroup {
                    data_files: files,
                    delete_files: vec![],
                }],
                additional_summary: Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state,
                )])),
            });
        }
        Ok(self)
    }

    /// Append files to the storage table
    pub fn delete(
        mut self,
        files: Vec<DataFile>,
        refresh_state: RefreshState,
    ) -> Result<Self, Error> {
        let refresh_state = serde_json::to_string(&refresh_state)?;
        if let Some(ref mut operation) = self.storage_table_operations[APPEND_INDEX] {
            if let TableOperation::Append {
                branch: _,
                dsn_groups,
                additional_summary: old_lineage,
            } = operation
            {
                match dsn_groups.last_mut() {
                    Some(g) => g.delete_files.extend_from_slice(&files),
                    None => dsn_groups.push(DsnGroup {
                        data_files: vec![],
                        delete_files: files,
                    }),
                };
                *old_lineage = Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state.clone(),
                )]));
            }
        } else {
            self.storage_table_operations[APPEND_INDEX] = Some(TableOperation::Append {
                branch: self.branch.clone(),
                dsn_groups: vec![DsnGroup {
                    data_files: vec![],
                    delete_files: files,
                }],
                additional_summary: Some(HashMap::from_iter(vec![(
                    REFRESH_STATE.to_owned(),
                    refresh_state,
                )])),
            });
        }
        Ok(self)
    }

    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.materialized_view.catalog();

        let identifier = self.materialized_view.identifier().clone();

        let storage_table = self.materialized_view.storage_table().await?;

        let delete_data = if !self.storage_table_operations.is_empty() {
            let (mut table_requirements, mut table_updates) = (Vec::new(), Vec::new());

            // Save old metadata to be able to remove old data after a rewrite operation
            let delete_data = if self
                .storage_table_operations
                .iter()
                .flatten()
                .any(|x| matches!(x, TableOperation::Replace { .. }))
            {
                Some(storage_table.metadata().clone())
            } else {
                None
            };

            // Execute table operations
            for operation in self.storage_table_operations.into_iter().flatten() {
                let (requirement, update) = operation
                    .execute(storage_table.metadata(), storage_table.object_store())
                    .await?;

                if let Some(requirement) = requirement {
                    table_requirements.push(requirement);
                }
                table_updates.extend(update);
            }

            storage_table
                .catalog()
                .update_table(CommitTable {
                    identifier: storage_table.identifier().clone(),
                    requirements: table_requirements,
                    updates: table_updates,
                })
                .await?;

            delete_data
        } else {
            None
        };
        // Execute the view operations
        let (mut view_requirements, mut view_updates) = (Vec::new(), Vec::new());
        for operation in self.view_operations {
            let (requirement, update) = operation.execute(&self.materialized_view.metadata).await?;

            if let Some(requirement) = requirement {
                view_requirements.push(requirement);
            }
            view_updates.extend(update);
        }

        let new_matview = catalog
            .clone()
            .update_materialized_view(CommitView {
                identifier,
                requirements: view_requirements,
                updates: view_updates,
            })
            .await?;
        // Delete data files in case of a rewrite operation
        if let Some(old_metadata) = delete_data {
            delete_all_table_files(&old_metadata, storage_table.object_store()).await?;
        }
        *self.materialized_view = new_matview;
        Ok(())
    }
}
