/*!
 * Provides the [Relation] enum to refer to any queriable entity like a table or a view
*/

use serde::{self, Deserialize, Serialize};

use crate::model::table_metadata::TableMetadataEnum;
use crate::model::view_metadata::ViewMetadata;
use crate::table::Table;
use crate::view::View;
/// Enum for different types that can be queried like a table, for example view
pub enum Relation {
    /// An iceberg table
    Table(Table),
    /// An iceberg view
    View(View),
}

impl Relation {
    /// Return metadata location for relation.
    pub fn metadata_location(&self) -> &str {
        match self {
            Relation::Table(table) => table.metadata_location(),
            Relation::View(view) => view.metadata_location(),
        }
    }
}

/// Metadata of an iceberg relation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RelationMetadata {
    /// Table metadata
    Table(TableMetadataEnum),
    /// View metadata
    View(ViewMetadata),
}
