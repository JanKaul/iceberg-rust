/*!
 * Defines the [Transaction] type for views to perform multiple view [Operation]s with ACID guarantees.
*/

pub mod operation;
use iceberg_rust_spec::spec::{types::StructType, view_metadata::ViewRepresentation};

use crate::{catalog::commit::CommitView, error::Error};

use self::operation::Operation as ViewOperation;

use super::View;

/// Transactions let you perform a sequence of [Operation]s that can be committed to be performed with ACID guarantees.
pub struct Transaction<'view> {
    view: &'view mut View,
    operations: Vec<ViewOperation>,
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
    pub fn update_representations(
        mut self,
        representations: Vec<ViewRepresentation>,
        schema: StructType,
    ) -> Self {
        self.operations.push(ViewOperation::UpdateRepresentations {
            representations,
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
        let (mut requirements, mut updates) = (Vec::new(), Vec::new());
        for operation in self.operations {
            let (requirement, update) = operation.execute(&self.view.metadata).await?;

            if let Some(requirement) = requirement {
                requirements.push(requirement);
            }
            updates.extend(update);
        }
        let new_view = catalog
            .clone()
            .update_view(CommitView {
                identifier,
                requirements,
                updates,
            })
            .await?;
        *self.view = new_view;
        Ok(())
    }
}
