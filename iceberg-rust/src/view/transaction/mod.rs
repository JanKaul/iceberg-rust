/*!
 * Defines the [Transaction] type for views to perform multiple view [Operation]s with ACID guarantees.
*/

use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

pub mod operation;
use iceberg_rust_spec::{
    spec::{types::StructType, view_metadata::ViewRepresentation},
    util::strip_prefix,
};

use crate::{
    catalog::{bucket::parse_bucket, tabular::Tabular},
    error::Error,
};

use self::operation::Operation as ViewOperation;

use super::View;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    view: &'view mut View,
    operations: Vec<ViewOperation<Option<()>>>,
    branch: Option<String>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut View, branch: Option<&str>) -> Self {
        Transaction {
            view,
            operations: vec![],
            branch: branch.map(ToString::to_string),
        }
    }
    /// Update the schmema of the view
    pub fn update_representation(
        mut self,
        representation: ViewRepresentation,
        schema: StructType,
    ) -> Self {
        self.operations.push(ViewOperation::UpdateRepresentation {
            representation,
            schema,
            branch: self.branch.clone(),
        });
        self
    }
    /// Update view properties
    pub fn update_properties(mut self, entries: Vec<(String, String)>) -> Self {
        self.operations
            .push(ViewOperation::UpdateProperties(entries));
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.view.catalog();

        let identifier = self.view.identifier().clone();
        // Execute the table operations
        let view = futures::stream::iter(self.operations)
            .map(Ok::<_, Error>)
            .try_fold(self.view, |view, op| async move {
                op.execute(&mut view.metadata).await?;
                Ok(view)
            })
            .await?;
        let bucket = parse_bucket(&view.metadata.location)?;
        let object_store = catalog.object_store(bucket);
        let location = &&view.metadata().location;
        let transaction_uuid = Uuid::new_v4();
        let version = &&view.metadata().current_version_id;
        let metadata_json = serde_json::to_string(&view.metadata())?;
        let metadata_file_location = location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &transaction_uuid.to_string()
            + ".metadata.json";
        object_store
            .put(
                &strip_prefix(&metadata_file_location).into(),
                metadata_json.into(),
            )
            .await?;
        let previous_metadata_file_location = view.metadata_location();
        if let Tabular::View(new_view) = catalog
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
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
