/*!
 * Defines the [Transaction] type for materialized views to perform multiple [Operation]s with ACID guarantees.
*/

use futures::StreamExt;
use object_store::path::Path;
use uuid::Uuid;

use crate::{
    catalog::relation::Tabular,
    error::Error,
    spec::{materialized_view_metadata::MaterializedViewRepresentation, schema::Schema},
    view::transaction::operation::Operation as ViewOperation,
};

use super::MaterializedView;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    materialized_view: &'view mut MaterializedView,
    operations: Vec<ViewOperation<MaterializedViewRepresentation>>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut MaterializedView) -> Self {
        Transaction {
            materialized_view: view,
            operations: vec![],
        }
    }
    /// Update the schmema of the view
    pub fn update_representation(
        mut self,
        representation: MaterializedViewRepresentation,
        schema: Schema,
    ) -> Self {
        self.operations.push(ViewOperation::UpdateRepresentation {
            representation,
            schema,
        });
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.materialized_view.catalog();
        let object_store = catalog.object_store();
        let identifier = self.materialized_view.identifier().clone();
        // Execute the table operations
        let materialized_view = futures::stream::iter(self.operations)
            .fold(
                Ok::<&mut MaterializedView, Error>(self.materialized_view),
                |view, op| async move {
                    let view = view?;
                    op.execute(&mut view.metadata).await?;
                    Ok(view)
                },
            )
            .await?;

        let location = &&materialized_view.metadata().location;
        let transaction_uuid = Uuid::new_v4();
        let version = &&materialized_view.metadata().current_version_id;
        let metadata_json = serde_json::to_string(&materialized_view.metadata())?;
        let metadata_file_location: Path = (location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &transaction_uuid.to_string()
            + ".metadata.json")
            .into();
        object_store
            .put(&metadata_file_location, metadata_json.into())
            .await?;
        let previous_metadata_file_location = materialized_view.metadata_location();
        if let Tabular::MaterializedView(new_mv) = catalog
            .clone()
            .update_table(
                identifier,
                metadata_file_location.as_ref(),
                previous_metadata_file_location,
            )
            .await?
        {
            *materialized_view = new_mv;
            Ok(())
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
