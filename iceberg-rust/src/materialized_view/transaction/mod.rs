/*!
 * Defines the [Transaction] type for materialized views to perform multiple [Operation]s with ACID guarantees.
*/

use std::collections::HashMap;

use iceberg_rust_spec::{
    spec::{
        manifest::DataFile, snapshot::Lineage, table_metadata::new_metadata_location,
        types::StructType, view_metadata::ViewRepresentation,
    },
    util::strip_prefix,
};

use crate::{
    catalog::{
        commit::{apply_table_updates, check_table_requirements, CommitView},
        tabular::Tabular,
    },
    error::Error,
    table::{
        delete_files,
        transaction::{operation::Operation as TableOperation, REWRITE_KEY},
    },
    view::transaction::operation::Operation as ViewOperation,
};

use super::MaterializedView;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    materialized_view: &'view mut MaterializedView,
    view_operations: Vec<ViewOperation<String>>,
    storage_table_operations: HashMap<String, TableOperation>,
    branch: Option<String>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut MaterializedView, branch: Option<&str>) -> Self {
        Transaction {
            materialized_view: view,
            view_operations: vec![],
            storage_table_operations: HashMap::new(),
            branch: branch.map(ToString::to_string),
        }
    }

    /// Update the schmema of the view
    pub fn update_representation(
        mut self,
        representation: ViewRepresentation,
        schema: StructType,
    ) -> Self {
        self.view_operations
            .push(ViewOperation::UpdateRepresentation {
                representation,
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

    /// Update materialization
    pub fn update_materialization(mut self, materialization: &str) -> Self {
        self.view_operations
            .push(ViewOperation::UpdateMaterialization(
                materialization.to_owned(),
            ));
        self
    }

    /// Perform full refresh operation
    pub fn full_refresh(mut self, files: Vec<DataFile>, lineage: Lineage) -> Self {
        self.storage_table_operations
            .entry(REWRITE_KEY.to_owned())
            .and_modify(|mut x| {
                if let TableOperation::Rewrite {
                    branch: _,
                    files: old,
                    lineage: old_lineage,
                } = &mut x
                {
                    old.extend_from_slice(&files);
                    *old_lineage = Some(lineage.clone());
                }
            })
            .or_insert(TableOperation::Rewrite {
                branch: self.branch.clone(),
                files,
                lineage: Some(lineage),
            });
        let materialization = new_metadata_location((self.materialized_view.metadata()).into());
        self.view_operations
            .push(ViewOperation::UpdateMaterialization(
                materialization.to_owned(),
            ));
        self
    }

    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.materialized_view.catalog();

        let identifier = self.materialized_view.identifier().clone();

        let delete_data = if !self.storage_table_operations.is_empty() {
            let (mut table_requirements, mut table_updates) = (Vec::new(), Vec::new());

            let mut storage_table_metadata =
                self.materialized_view.storage_table().await?.table_metadata;

            // Save old metadata to be able to remove old data after a rewrite operation
            let delete_data = if self.storage_table_operations.values().any(|x| match x {
                TableOperation::Rewrite {
                    branch: _,
                    files: _,
                    lineage: _,
                } => true,
                _ => false,
            }) {
                Some(storage_table_metadata.clone())
            } else {
                None
            };

            // Execute table operations
            for operation in self.storage_table_operations.into_values() {
                let (requirement, update) = operation
                    .execute(
                        &storage_table_metadata,
                        self.materialized_view.object_store(),
                    )
                    .await?;

                if let Some(requirement) = requirement {
                    table_requirements.push(requirement);
                }
                table_updates.extend(update);
            }

            if !check_table_requirements(&table_requirements, &storage_table_metadata) {
                return Err(Error::InvalidFormat(
                    "Table requirements not valid".to_owned(),
                ));
            }

            apply_table_updates(&mut storage_table_metadata, table_updates)?;

            let metadata_location = self
                .view_operations
                .iter()
                .find_map(|x| match x {
                    ViewOperation::UpdateMaterialization(materialization) => Some(materialization),
                    _ => None,
                })
                .ok_or(Error::NotFound(
                    "Storage table".to_owned(),
                    "pointer".to_owned(),
                ))?;

            self.materialized_view
                .object_store()
                .put(
                    &strip_prefix(&metadata_location).into(),
                    serde_json::to_string(&storage_table_metadata)?.into(),
                )
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

        if let Tabular::MaterializedView(new_matview) = catalog
            .clone()
            .update_view(CommitView {
                identifier,
                requirements: view_requirements,
                updates: view_updates,
            })
            .await?
        {
            // Delete data files in case of a rewrite operation
            if let Some(old_metadata) = delete_data {
                delete_files(&old_metadata, self.materialized_view.object_store()).await?;
            }
            *self.materialized_view = new_matview;
            Ok(())
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
