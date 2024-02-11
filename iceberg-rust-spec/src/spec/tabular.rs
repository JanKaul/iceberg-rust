/*! Enum for Metadata of Table, View or Materialized View
*/

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    materialized_view_metadata::MaterializedViewMetadata, table_metadata::TableMetadata,
    view_metadata::ViewMetadata,
};
/// Metadata of an iceberg relation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum TabularMetadata {
    /// Table metadata
    Table(TableMetadata),
    /// View metadata
    View(ViewMetadata),
    /// Materialized view metadata
    MaterializedView(MaterializedViewMetadata),
}

impl TabularMetadata {
    pub fn as_ref<'a>(&'a self) -> TabularMetadataRef<'a> {
        match self {
            TabularMetadata::Table(table) => TabularMetadataRef::Table(table),
            TabularMetadata::View(view) => TabularMetadataRef::View(view),
            TabularMetadata::MaterializedView(matview) => {
                TabularMetadataRef::MaterializedView(matview)
            }
        }
    }
}

impl From<TableMetadata> for TabularMetadata {
    fn from(value: TableMetadata) -> Self {
        TabularMetadata::Table(value)
    }
}

impl From<ViewMetadata> for TabularMetadata {
    fn from(value: ViewMetadata) -> Self {
        TabularMetadata::View(value)
    }
}

impl From<MaterializedViewMetadata> for TabularMetadata {
    fn from(value: MaterializedViewMetadata) -> Self {
        TabularMetadata::MaterializedView(value)
    }
}

pub enum TabularMetadataRef<'a> {
    /// Table metadata
    Table(&'a TableMetadata),
    /// View metadata
    View(&'a ViewMetadata),
    /// Materialized view metadata
    MaterializedView(&'a MaterializedViewMetadata),
}

impl<'a> TabularMetadataRef<'a> {
    /// Get uuid of tabular
    pub fn uuid(&self) -> &Uuid {
        match self {
            TabularMetadataRef::Table(table) => &table.table_uuid,
            TabularMetadataRef::View(view) => &view.view_uuid,
            TabularMetadataRef::MaterializedView(matview) => &matview.view_uuid,
        }
    }
    /// Get location for tabular
    pub fn location(&self) -> &str {
        match self {
            TabularMetadataRef::Table(table) => &table.location,
            TabularMetadataRef::View(view) => &view.location,
            TabularMetadataRef::MaterializedView(matview) => &matview.location,
        }
    }
    /// Get sequence number for tabular
    pub fn sequence_number(&self) -> i64 {
        match self {
            TabularMetadataRef::Table(table) => table.last_sequence_number,
            TabularMetadataRef::View(view) => view.current_version_id,
            TabularMetadataRef::MaterializedView(matview) => matview.current_version_id,
        }
    }
}

impl<'a> From<&'a TableMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a TableMetadata) -> Self {
        TabularMetadataRef::Table(value)
    }
}

impl<'a> From<&'a ViewMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a ViewMetadata) -> Self {
        TabularMetadataRef::View(value)
    }
}

impl<'a> From<&'a MaterializedViewMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a MaterializedViewMetadata) -> Self {
        TabularMetadataRef::MaterializedView(value)
    }
}
