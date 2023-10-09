/*!
 * A Struct for the materialized view metadata   
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{table_metadata::VersionNumber, view_metadata::GeneralViewMetadata};

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<MaterializedViewRepresentation>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Fields for the version 2 of the view metadata.
pub enum MaterializedViewRepresentation {
    #[serde(rename = "sql_materialized")]
    /// This type of representation stores the original view definition in SQL and its SQL dialect.
    SqlMaterialized {
        /// A string representing the original view definition in SQL
        sql: String,
        /// An integer version number for the materialized view format. Currently, this must be 1. Implementations must throw an exception if the materialized view's version is higher than the supported version.
        format_version: VersionNumber<1>,
        /// A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect.
        dialect: String,
        /// Pointer to the storage table
        storage_table_pointer: String,
        /// ID of the view’s schema when the version was created
        schema_id: Option<i64>,
        /// A string specifying the catalog to use when the table or view references in the view definition do not contain an explicit catalog.
        default_catalog: Option<String>,
        /// The namespace to use when the table or view references in the view definition do not contain an explicit namespace.
        /// Since the namespace may contain multiple parts, it is serialized as a list of strings.
        default_namespace: Option<Vec<String>>,
        /// A list of strings of field aliases optionally specified in the create view statement.
        /// The list should have the same length as the schema’s top level fields. See the example below.
        field_aliases: Option<Vec<String>>,
        /// A list of strings of field comments optionally specified in the create view statement.
        /// The list should have the same length as the schema’s top level fields. See the example below.
        field_docs: Option<Vec<String>>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct Freshness {
    /// Version id of the materialized view when the refresh operation was performed.
    version_id: i64,
    /// Map from references in the sql expression to snapshot_ids of the last refresh operation
    base_tables: HashMap<String, i64>,
}
