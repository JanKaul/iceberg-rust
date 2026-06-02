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
    /// Parses the optional spec-id → arrow-source-id mapping stored in the
    /// view's properties under `ARROW_FIELD_IDS_PROPERTY` (see
    /// `iceberg_rust_spec::spec::view_metadata::ARROW_FIELD_IDS_PROPERTY`).
    ///
    /// Encoding: `"<spec_id>:<arrow_id>,..."`. Returns an empty map when the
    /// property is absent or the value cannot be parsed.
    pub fn arrow_field_ids(&self) -> std::collections::HashMap<i32, i32> {
        parse_arrow_field_ids(
            self.metadata
                .properties
                .get(iceberg_rust_spec::spec::view_metadata::ARROW_FIELD_IDS_PROPERTY),
        )
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

/// Encodes a spec-id → arrow-source-id mapping into the comma-separated
/// `"<spec_id>:<arrow_id>,..."` form expected by `ARROW_FIELD_IDS_PROPERTY`.
/// Order is deterministic (ascending by spec id) so identical mappings
/// produce identical property values across runs.
pub fn encode_arrow_field_ids(mapping: &std::collections::HashMap<i32, i32>) -> String {
    let mut entries: Vec<(&i32, &i32)> = mapping.iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    entries
        .into_iter()
        .map(|(spec, arrow)| format!("{spec}:{arrow}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Parses the comma-separated `"<spec_id>:<arrow_id>,..."` value of
/// `ARROW_FIELD_IDS_PROPERTY` into a spec-id → arrow-source-id map.
/// Returns an empty map for `None` input or any unparseable entry — callers
/// treat absence and parse failure identically (fall back to the spec id
/// as the arrow id).
pub fn parse_arrow_field_ids(value: Option<&String>) -> std::collections::HashMap<i32, i32> {
    let mut out = std::collections::HashMap::new();
    let Some(raw) = value else {
        return out;
    };
    for entry in raw.split(',').filter(|s| !s.is_empty()) {
        let (spec_str, arrow_str) = match entry.split_once(':') {
            Some(pair) => pair,
            None => return std::collections::HashMap::new(),
        };
        let (Ok(spec), Ok(arrow)) = (spec_str.parse::<i32>(), arrow_str.parse::<i32>()) else {
            return std::collections::HashMap::new();
        };
        out.insert(spec, arrow);
    }
    out
}

#[cfg(test)]
mod arrow_field_ids_tests {
    use super::{encode_arrow_field_ids, parse_arrow_field_ids};

    #[test]
    fn round_trip_single_table() {
        let mapping = [(1, 1), (2, 3)].into_iter().collect();
        let encoded = encode_arrow_field_ids(&mapping);
        assert_eq!(encoded, "1:1,2:3");
        let decoded = parse_arrow_field_ids(Some(&encoded));
        assert_eq!(decoded, mapping);
    }

    #[test]
    fn round_trip_self_join_collision() {
        // Two view spec ids both sourced from arrow id 1 (e.g. a JOIN b on
        // the same base table).
        let mapping = [(1, 1), (2, 1)].into_iter().collect();
        let encoded = encode_arrow_field_ids(&mapping);
        assert_eq!(encoded, "1:1,2:1");
        let decoded = parse_arrow_field_ids(Some(&encoded));
        assert_eq!(decoded, mapping);
    }

    #[test]
    fn absent_property_yields_empty_map() {
        assert!(parse_arrow_field_ids(None).is_empty());
    }

    #[test]
    fn malformed_property_yields_empty_map() {
        assert!(parse_arrow_field_ids(Some(&"not-a-pair".to_string())).is_empty());
        assert!(parse_arrow_field_ids(Some(&"1:x,2:3".to_string())).is_empty());
    }
}
