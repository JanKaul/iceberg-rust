/*!
 * Defines the [View] struct that represents an iceberg view.
*/

use anyhow::Result;

use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    catalog::{identifier::Identifier, Catalog},
    spec::{schema::Schema, view_metadata::ViewMetadata},
};

use self::transaction::Transaction as ViewTransaction;

pub mod transaction;
pub mod view_builder;

/// An iceberg view
pub struct View {
    /// Type of the View, either filesystem or metastore.
    identifier: Identifier,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: ViewMetadata,
    /// Path to the current metadata location
    metadata_location: String,
    /// Catalog of the table
    catalog: Arc<dyn Catalog>,
}

/// Public interface of the table.
impl View {
    /// Create a new metastore view
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: ViewMetadata,
        metadata_location: &str,
    ) -> Result<Self> {
        Ok(View {
            identifier,
            metadata,
            metadata_location: metadata_location.to_string(),
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
        self.catalog.object_store()
    }
    /// Get the schema of the view
    pub fn schema(&self) -> Result<&Schema> {
        self.metadata.current_schema()
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self) -> ViewTransaction {
        ViewTransaction::new(self)
    }
}

/// Private interface of the view.
impl View {
    /// Increment the version number of the view. Is typically used when commiting a new view transaction.
    pub(crate) fn increment_version_number(&mut self) {
        self.metadata.current_version_id += 1;
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, Catalog},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        view::view_builder::ViewBuilder,
    };

    #[tokio::test]
    async fn test_increment_sequence_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("test", object_store).unwrap());
        let identifier = Identifier::parse("test.view1").unwrap();
        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType::new(vec![
                StructField {
                    id: 1,
                    name: "one".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
                StructField {
                    id: 2,
                    name: "two".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
            ]),
        };
        let mut builder = ViewBuilder::new(
            "SELECT trip_distance FROM nyc_taxis",
            schema,
            identifier,
            catalog,
        )
        .unwrap();
        builder.location("test/view1");
        let mut view = builder.build().await.unwrap();

        let metadata_location1 = view.metadata_location().to_owned();

        let transaction = view.new_transaction();
        transaction.commit().await.unwrap();
        let metadata_location2 = view.metadata_location();
        assert_ne!(metadata_location1, metadata_location2);
    }
}
