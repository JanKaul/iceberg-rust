/*!
 * Materialized view transactions
*/

use crate::model::materialized_view_metadata::MaterializedViewMetadataInView;

use self::operation::Operation as MaterializedViewOperation;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use object_store::path::Path;
use uuid::Uuid;

use super::MaterializedView;

pub mod operation;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct MaterializedViewTransaction<'mv> {
    materialized_view: &'mv mut MaterializedView,
    operations: Vec<MaterializedViewOperation>,
}

impl<'mv> MaterializedViewTransaction<'mv> {
    /// Create a transaction for the given view.
    pub fn new(mv: &'mv mut MaterializedView) -> Self {
        MaterializedViewTransaction {
            materialized_view: mv,
            operations: vec![],
        }
    }
    /// Update the location of the view
    pub fn update_location(mut self, location: &str) -> Self {
        self.operations
            .push(MaterializedViewOperation::UpdateLocation(
                location.to_owned(),
            ));
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<()> {
        // Get materialized view metadata from view properties
        let metadata_in_view = self
            .materialized_view
            .view
            .metadata()
            .properties()
            .and_then(|x| x.get("materialized_view_metadata"))
            .ok_or_else(|| anyhow!("View is missing materialized_view_metadata field."))?;
        let mut metadata_in_view: MaterializedViewMetadataInView =
            serde_json::from_str(metadata_in_view)?;
        // Execute the table operations
        let (mut view_transaction, table_transaction) = futures::stream::iter(self.operations)
            .fold(
                Ok::<_, anyhow::Error>((
                    self.materialized_view.view.new_transaction(),
                    self.materialized_view.storage_table.new_transaction(),
                )),
                |view, op| async move {
                    let (mut view_transaction, mut table_transaction) = view?;
                    op.execute(&mut view_transaction, &mut table_transaction)
                        .await?;
                    Ok((view_transaction, table_transaction))
                },
            )
            .await?;
        // Execute storage table operations
        let table = table_transaction.execute().await?;
        // Write storage table metadata to metadata file
        let storage_table_location = table.metadata().location();
        let transaction_uuid = Uuid::new_v4();
        let table_version = &table.metadata().last_sequence_number();
        let storage_table_metadata_location: Path = (storage_table_location.to_string()
            + "/metadata/"
            + &table_version.to_string()
            + "-"
            + &transaction_uuid.to_string()
            + ".metadata.json")
            .into();
        let storage_table_metadata_json =
            serde_json::to_string(&table.metadata()).map_err(|err| anyhow!(err.to_string()))?;
        table
            .object_store()
            .put(
                &storage_table_metadata_location,
                storage_table_metadata_json.into(),
            )
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        // Update storage table location in view metadata
        match &mut metadata_in_view {
            MaterializedViewMetadataInView::V1(metadata) => {
                metadata.storage_table_location = storage_table_metadata_location.to_string();
            }
        }
        view_transaction.update_property_mut(
            "materialized_view_metadata",
            &serde_json::to_string(&metadata_in_view).map_err(|err| anyhow!(err.to_string()))?,
        );
        // Perform view transaction
        view_transaction.commit().await
    }
}
