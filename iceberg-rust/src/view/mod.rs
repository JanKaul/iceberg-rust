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
    /// Gets the unique identifier for this view in the catalog
    ///
    /// The identifier contains:
    /// - The namespace path for the view
    /// - The view name
    ///
    /// # Returns
    /// * `&Identifier` - A reference to this view's identifier
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Gets the catalog that manages this view
    ///
    /// The catalog handles:
    /// - View metadata storage and retrieval
    /// - Schema management
    /// - View versioning
    /// - Access control
    ///
    /// # Returns
    /// * `Arc<dyn Catalog>` - A thread-safe reference to the catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Gets the object store for this view's storage location
    ///
    /// The object store provides:
    /// - Access to the underlying storage system (S3, local filesystem, etc)
    /// - Read/write operations for view data and metadata
    /// - Storage-specific configuration and credentials
    ///
    /// # Returns
    /// * `Arc<dyn ObjectStore>` - A thread-safe reference to the configured object store
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    /// Gets the current schema for this view, optionally for a specific branch
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the schema for. If None, returns the main branch schema
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The current schema for the view/branch, or an error if not found
    ///
    /// The schema defines the structure of the view's output, including:
    /// - Column names and types
    /// - Column IDs and documentation
    /// - Whether columns are required/optional
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Gets the underlying view metadata that defines this view
    ///
    /// The metadata contains:
    /// - View UUID and format version
    /// - Storage location information
    /// - Version history and schema evolution
    /// - View properties and configurations
    /// - SQL representations and other view definitions
    ///
    /// # Returns
    /// * `&ViewMetadata` - A reference to the view's metadata
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }
    //AI! Write documentation
    pub fn new_transaction(&mut self, branch: Option<&str>) -> ViewTransaction {
        ViewTransaction::new(self, branch)
    }
}
