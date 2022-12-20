/*!
 * Defines the [Transaction] type for views to perform multiple view [Operation]s with ACID guarantees.
*/

use anyhow::{anyhow, Result};
use futures::StreamExt;
use object_store::path::Path;
use uuid::Uuid;

pub mod operation;

use crate::{catalog::relation::Relation, model::schema::Schema};

use self::operation::Operation as ViewOperation;

use super::View;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    view: &'view mut View,
    operations: Vec<ViewOperation>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut View) -> Self {
        Transaction {
            view,
            operations: vec![],
        }
    }
    /// Update the schmema of the view
    pub fn update_schema(mut self, schema: Schema) -> Self {
        self.operations.push(ViewOperation::UpdateSchema(schema));
        self
    }
    /// Update the location of the view
    pub fn update_location(mut self, location: &str) -> Self {
        self.operations
            .push(ViewOperation::UpdateLocation(location.to_owned()));
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<()> {
        // Before executing the transactions operations, update the version number
        self.view.increment_version_number();
        // Execute the table operations
        let view = futures::stream::iter(self.operations)
            .fold(
                Ok::<&mut View, anyhow::Error>(self.view),
                |view, op| async move {
                    let view = view?;
                    op.execute(view).await?;
                    Ok(view)
                },
            )
            .await?;
        // Write the new state to the object store
        match (view.catalog(), view.identifier()) {
            // In case of a metastore view, write the metadata to object srorage and use the catalog to perform the atomic swap
            (Some(catalog), Some(identifier)) => {
                let object_store = catalog.object_store();
                let location = &view.metadata().location();
                let transaction_uuid = Uuid::new_v4();
                let version = &view.metadata().current_version_id();
                let metadata_json = serde_json::to_string(&view.metadata())
                    .map_err(|err| anyhow!(err.to_string()))?;
                let metadata_file_location: Path = (location.to_string()
                    + "/metadata/"
                    + &version.to_string()
                    + "-"
                    + &transaction_uuid.to_string()
                    + ".metadata.json")
                    .into();
                object_store
                    .put(&metadata_file_location, metadata_json.into())
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                let previous_metadata_file_location = view.metadata_location();
                if let Relation::View(new_view) = catalog
                    .clone()
                    .update_table(
                        identifier.clone(),
                        metadata_file_location.as_ref(),
                        previous_metadata_file_location,
                    )
                    .await?
                {
                    *view = new_view;
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Updating the table for the transaction didn't return a table."
                    ))
                }
            }
            // In case of a filesystem table, write the metadata to the object storage and perform the atomic swap of the metadata file
            (_, _) => {
                let object_store = view.object_store();
                let location = &view.metadata().location();
                let uuid = Uuid::new_v4();
                let version = &view.metadata().current_version_id();
                let metadata_json = serde_json::to_string(&view.metadata())
                    .map_err(|err| anyhow!(err.to_string()))?;
                let temp_path: Path =
                    (location.to_string() + "/metadata/" + &uuid.to_string() + ".metadata.json")
                        .into();
                let final_path: Path = (location.to_string()
                    + "/metadata/v"
                    + &version.to_string()
                    + ".metadata.json")
                    .into();
                object_store
                    .put(&temp_path, metadata_json.into())
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                object_store
                    .copy_if_not_exists(&temp_path, &final_path)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                object_store
                    .delete(&temp_path)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                let new_view = View::load_file_system_view(location, &object_store).await?;
                *view = new_view;
                Ok(())
            }
        }
    }
}
