/*!
 * Provides the [Relation] enum to refer to any queriable entity like a table or a view
*/

use std::sync::Arc;

use object_store::ObjectStore;
use serde::{self, Deserialize, Serialize};

use crate::error::Error;
use crate::materialized_view::MaterializedView;
use crate::spec::materialized_view_metadata::MaterializedViewMetadata;
use crate::spec::table_metadata::TableMetadata;
use crate::spec::view_metadata::ViewMetadata;
use crate::table::Table;
use crate::view::View;

use super::Catalog;

#[derive(Debug)]
/// Enum for different types that can be queried like a table, for example view
pub enum Tabular {
    /// An iceberg table
    Table(Table),
    /// An iceberg view
    View(View),
    /// An iceberg materialized view
    MaterializedView(MaterializedView),
}

impl Tabular {
    #[inline]
    /// Return metadata location for relation.
    pub fn metadata_location(&self) -> &str {
        match self {
            Tabular::Table(table) => table.metadata_location(),
            Tabular::View(view) => view.metadata_location(),
            Tabular::MaterializedView(mv) => mv.metadata_location(),
        }
    }

    #[inline]
    /// Return catalog for relation.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        match self {
            Tabular::Table(table) => table.catalog(),
            Tabular::View(view) => view.catalog(),
            Tabular::MaterializedView(mv) => mv.catalog(),
        }
    }

    /// Reload relation from catalog
    pub async fn reload(&mut self) -> Result<(), Error> {
        match self {
            Tabular::Table(table) => {
                let new = if let Tabular::Table(table) =
                    table.catalog().load_table(table.identifier()).await?
                {
                    Ok(table)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(table, new);
            }
            Tabular::View(view) => {
                let new = if let Tabular::View(view) =
                    view.catalog().load_table(view.identifier()).await?
                {
                    Ok(view)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(view, new);
            }
            Tabular::MaterializedView(matview) => {
                let new = if let Tabular::MaterializedView(matview) =
                    matview.catalog().load_table(matview.identifier()).await?
                {
                    Ok(matview)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(matview, new);
            }
        };
        Ok(())
    }
}

/// Metadata of an iceberg relation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TabularMetadata {
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
) -> Result<TabularMetadata, Error> {
    let bytes = object_store
        .get(&metadata_location.into())
        .await?
        .bytes()
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}
