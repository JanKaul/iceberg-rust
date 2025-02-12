/*!
 * Defines the [View] struct that represents an iceberg view.
*/

use std::sync::Arc;

use iceberg_rust_spec::spec::{schema::Schema, view_metadata::ViewMetadata};
use object_store::ObjectStore;

use crate::{
    catalog::{create::CreateViewBuilder, identifier::Identifier, Catalog},
    error::Error,
    object_store::Bucket,
};

use self::transaction::Transaction as ViewTransaction;

pub mod transaction;

#[derive(Debug, Clone)]
/// An Iceberg view provides a logical view over underlying data with schema evolution and versioning
///
/// Views store:
/// - SQL query or other representation of the view logic
/// - Schema evolution history
/// - Version history for view definitions
/// - Properties for configuration
/// - Metadata about view location and catalog
///
/// Views can be either filesystem-based or managed by a metastore catalog.
/// They support transactions for atomic updates to their definition and properties.
///
/// The View struct provides the main interface for:
/// - Creating and managing views
/// - Accessing view metadata and schemas
/// - Starting transactions for atomic updates
/// - Interacting with the underlying storage system
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
    /// Creates a new builder for configuring and creating an Iceberg view
    ///
    /// Returns a `CreateViewBuilder` that provides a fluent interface for:
    /// - Setting the view name and location
    /// - Configuring view properties
    /// - Defining the view schema
    /// - Setting SQL or other view representations
    /// - Specifying the catalog and namespace
    ///
    /// # Returns
    /// * `CreateViewBuilder<Option<()>>` - A builder for creating new views
    pub fn builder() -> CreateViewBuilder<Option<()>> {
        CreateViewBuilder::default()
    }
    /// Creates a new Iceberg view instance with the given identifier, catalog and metadata
    ///
    /// # Arguments
    /// * `identifier` - The unique identifier for this view in the catalog
    /// * `catalog` - The catalog that will manage this view
    /// * `metadata` - The view metadata containing schema, versions, and other view information
    ///
    /// # Returns
    /// * `Result<Self, Error>` - The new view instance or an error if creation fails
    ///
    /// This is typically called by catalog implementations rather than directly by users.
    /// For creating new views, use the `builder()` method instead.
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
    //AI! write documentation
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
