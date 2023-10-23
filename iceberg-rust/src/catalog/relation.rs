/*!
 * Provides the [Relation] enum to refer to any queriable entity like a table or a view
*/

use std::sync::Arc;

use object_store::ObjectStore;
use serde::{self, Deserialize, Serialize};

use crate::materialized_view::MaterializedView;
use crate::spec::materialized_view_metadata::MaterializedViewMetadata;
use crate::spec::table_metadata::TableMetadata;
use crate::spec::view_metadata::ViewMetadata;
use crate::table::Table;
use crate::view::View;

#[derive(Debug)]
/// Enum for different types that can be queried like a table, for example view
pub enum Relation {
    /// An iceberg table
    Table(Table),
    /// An iceberg view
    View(View),
    /// An iceberg materialized view
    MaterializedView(MaterializedView),
}

impl Relation {
    /// Return metadata location for relation.
    pub fn metadata_location(&self) -> &str {
        match self {
            Relation::Table(table) => table.metadata_location(),
            Relation::View(view) => view.metadata_location(),
            Relation::MaterializedView(mv) => mv.metadata_location(),
        }
    }
}

/// Metadata of an iceberg relation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RelationMetadata {
    /// Table metadata
    Table(TableMetadata),
    /// View metadata
    View(ViewMetadata),
    /// Materialized view metadata
    MaterializedView(MaterializedViewMetadata),
}

/// Fetch metadata of a tabular(table, view, materialized view) structure from an object_store
pub async fn get_tabular_metadata(
    metadata_location: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RelationMetadata, anyhow::Error> {
    let bytes = object_store
        .get(&metadata_location.into())
        .await?
        .bytes()
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}
