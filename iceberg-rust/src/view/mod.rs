//! View management for Apache Iceberg tables
//!
//! This module provides the core functionality for working with Iceberg views:
//!
//! - Creating and managing views through the [View] struct
//! - Atomic updates via [transaction] support
//! - Schema evolution and versioning
//! - View metadata management
//! - Integration with catalogs and object stores
//!
//! Views provide a logical abstraction over underlying data, supporting:
//! - SQL-based view definitions
//! - Schema evolution tracking
//! - Version history
//! - Properties and configuration
//! - Metadata management
//!
//! # Example
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use iceberg_rust::view::View;
//!
//! // Create a new view using the builder pattern
//! let mut view = View::builder()
//!     .with_name("example_view")
//!     .with_schema(/* ... */)
//!     .build()
//!     .await?;
//!
//! // Start a transaction to update the view
//! view.new_transaction(None)
//!     .update_properties(vec![("comment".into(), "Example view".into())])
//!     .commit()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use iceberg_rust_spec::spec::{schema::Schema, view_metadata::ViewMetadata};

use crate::{
    catalog::{create::CreateViewBuilder, identifier::Identifier, Catalog},
    error::Error,
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
    /// Creates a new transaction for performing atomic updates to this view
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to create the transaction for. If None, uses the main branch
    ///
    /// # Returns
    /// * `ViewTransaction` - A new transaction that can be used to:
    ///   - Update view representations and schemas
    ///   - Modify view properties
    ///   - Commit changes atomically
    ///
    /// Transactions ensure that all changes are applied atomically with ACID guarantees.
    /// Multiple operations can be chained and will be committed together.
    pub fn new_transaction(&mut self, branch: Option<&str>) -> ViewTransaction<'_> {
        ViewTransaction::new(self, branch)
    }
}
