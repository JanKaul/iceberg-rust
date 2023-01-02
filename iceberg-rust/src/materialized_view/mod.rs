/*!
 * Materialized View
*/

pub mod transaction;

use std::sync::Arc;

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::data_types::StructType,
    table::Table,
    view::View,
};

use object_store::ObjectStore;
use transaction::MaterializedViewTransaction;

/// Iceberg materialized view
pub struct MaterializedView {
    view: View,
    storage_table: Table,
}

/// Public interface of the table.
impl MaterializedView {
    // /// Create a new metastore view
    // pub async fn new_metastore_materialized_view(
    //     identifier: Identifier,
    //     catalog: Arc<dyn Catalog>,
    //     metadata: ViewMetadata,
    //     metadata_location: &str,
    // ) -> Result<Self> {
    //     Ok(View {
    //         table_type: TableType::Metastore(identifier, catalog),
    //         metadata,
    //         metadata_location: metadata_location.to_string(),
    //     })
    // }
    // /// Load a filesystem view from an objectstore
    // pub async fn load_file_system_view(
    //     location: &str,
    //     object_store: &Arc<dyn ObjectStore>,
    // ) -> Result<Self> {
    //     let path: Path = (location.to_string() + "/metadata/").into();
    //     let files = object_store
    //         .list(Some(&path))
    //         .await
    //         .map_err(|err| anyhow!(err.to_string()))?;
    //     let version = files
    //         .fold(Ok::<i64, anyhow::Error>(0), |acc, x| async move {
    //             match (acc, x) {
    //                 (Ok(acc), Ok(object_meta)) => {
    //                     let name = object_meta
    //                         .location
    //                         .parts()
    //                         .last()
    //                         .ok_or_else(|| anyhow!("Metadata location path is empty."))?;
    //                     if name.as_ref().ends_with(".metadata.json") {
    //                         let version: i64 = name
    //                             .as_ref()
    //                             .trim_start_matches('v')
    //                             .trim_end_matches(".metadata.json")
    //                             .parse()?;
    //                         if version > acc {
    //                             Ok(version)
    //                         } else {
    //                             Ok(acc)
    //                         }
    //                     } else {
    //                         Ok(acc)
    //                     }
    //                 }
    //                 (Err(err), _) => Err(anyhow!(err.to_string())),
    //                 (_, Err(err)) => Err(anyhow!(err.to_string())),
    //             }
    //         })
    //         .await?;
    //     let metadata_location = path.to_string() + "/v" + &version.to_string() + ".metadata.json";
    //     let bytes = &object_store
    //         .get(&metadata_location.clone().into())
    //         .await
    //         .map_err(|err| anyhow!(err.to_string()))?
    //         .bytes()
    //         .await
    //         .map_err(|err| anyhow!(err.to_string()))?;
    //     let metadata: ViewMetadata = serde_json::from_str(
    //         std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
    //     )
    //     .map_err(|err| anyhow!(err.to_string()))?;
    //     Ok(View {
    //         metadata,
    //         table_type: TableType::FileSystem(Arc::clone(object_store)),
    //         metadata_location,
    //     })
    // }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem view.
    pub fn identifier(&self) -> Option<&Identifier> {
        self.view.identifier()
    }
    /// Get the catalog associated to the view. Returns None if the view is a filesystem view
    pub fn catalog(&self) -> Option<&Arc<dyn Catalog>> {
        self.view.catalog()
    }
    /// Get the object_store associated to the view
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.view.object_store()
    }
    /// Get the schema of the view
    pub fn schema(&self) -> Option<&StructType> {
        self.view.schema()
    }
    // /// Get the metadata of the view
    // pub fn metadata(&self) -> &ViewMetadata {
    //     &self.metadata
    // }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.view.metadata_location()
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self)
    }
}
