/*!
 * Defines the [View] struct that represents an iceberg view.
*/

use std::sync::Arc;

use iceberg_rust_spec::spec::{schema::Schema, view_metadata::ViewMetadata};
use object_store::ObjectStore;

use crate::{
    catalog::{bucket::Bucket, create::CreateViewBuilder, identifier::Identifier, Catalog},
    error::Error,
};

use self::transaction::Transaction as ViewTransaction;

pub mod transaction;

#[derive(Debug)]
/// An iceberg view
pub struct View {
    /// Type of the View, either filesystem or metastore.
    identifier: Identifier,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: ViewMetadata,
    /// Catalog of the table
    catalog: Arc<dyn Catalog>,
}

/// Public interface of the table.
impl View {
    /// Create a view builder
    pub fn builder() -> CreateViewBuilder<Option<()>> {
        CreateViewBuilder::default()
    }
    /// Create a new metastore view
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: ViewMetadata,
    ) -> Result<Self, Error> {
        Ok(View {
            identifier,
            metadata,
            catalog,
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem view.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Get the catalog associated to the view. Returns None if the view is a filesystem view
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Get the object_store associated to the view
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    /// Get the schema of the view
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self, branch: Option<&str>) -> ViewTransaction {
        ViewTransaction::new(self, branch)
    }
}
