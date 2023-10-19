/*!
 * Defines the [Transaction] type for views to perform multiple view [Operation]s with ACID guarantees.
*/

use anyhow::{anyhow, Result};
use futures::StreamExt;
use object_store::path::Path;
use uuid::Uuid;

pub mod operation;

use crate::{
    catalog::relation::Relation,
    spec::{schema::Schema, view_metadata::ViewRepresentation},
};

use self::operation::Operation as ViewOperation;

use super::View;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    view: &'view mut View,
    operations: Vec<ViewOperation<ViewRepresentation>>,
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
    pub fn update_representation(
        mut self,
        representation: ViewRepresentation,
        schema: Schema,
    ) -> Self {
        self.operations.push(ViewOperation::UpdateRepresentation {
            representation,
            schema,
        });
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<()> {
        let catalog = self.view.catalog();
        let object_store = catalog.object_store();
        let identifier = self.view.identifier().clone();
        // Before executing the transactions operations, update the version number
        self.view.increment_version_number();
        // Execute the table operations
        let view = futures::stream::iter(self.operations)
            .fold(
                Ok::<&mut View, anyhow::Error>(self.view),
                |view, op| async move {
                    let view = view?;
                    op.execute(&mut view.metadata).await?;
                    Ok(view)
                },
            )
            .await?;

        let location = &&view.metadata().location;
        let transaction_uuid = Uuid::new_v4();
        let version = &&view.metadata().current_version_id;
        let metadata_json =
            serde_json::to_string(&view.metadata()).map_err(|err| anyhow!(err.to_string()))?;
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
                identifier,
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
}
