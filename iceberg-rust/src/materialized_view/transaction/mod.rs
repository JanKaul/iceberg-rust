/*!
 * Defines the [Transaction] type for materialized views to perform multiple [Operation]s with ACID guarantees.
*/

use iceberg_rust_spec::spec::{types::StructType, view_metadata::ViewRepresentation};

use crate::{
    catalog::{commit::CommitView, tabular::Tabular},
    error::Error,
    view::transaction::operation::Operation as ViewOperation,
};

use super::MaterializedView;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    materialized_view: &'view mut MaterializedView,
    operations: Vec<ViewOperation<String>>,
    branch: Option<String>,
}

impl<'view> Transaction<'view> {
    /// Create a transaction for the given view.
    pub fn new(view: &'view mut MaterializedView, branch: Option<&str>) -> Self {
        Transaction {
            materialized_view: view,
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
    /// Update materialization
    pub fn update_materialization(mut self, materialization: &str) -> Self {
        self.operations.push(ViewOperation::UpdateMaterialization(
            materialization.to_owned(),
        ));
        self
    }
    /// Commit the transaction to perform the [Operation]s with ACID guarantees.
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.materialized_view.catalog();

        let identifier = self.materialized_view.identifier().clone();
        // Execute the table operations
        let (mut requirements, mut updates) = (Vec::new(), Vec::new());
        for operation in self.operations {
            let (requirement, update) = operation.execute(&self.materialized_view.metadata).await?;

            if let Some(requirement) = requirement {
                requirements.push(requirement);
            }
            updates.extend(update);
        }

        if let Tabular::MaterializedView(new_mv) = catalog
            .clone()
            .update_view(CommitView {
                identifier,
                requirements,
                updates,
            })
            .await?
        {
            *self.materialized_view = new_mv;
            Ok(())
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
