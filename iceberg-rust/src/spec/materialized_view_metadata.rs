/*!
 * A Struct for the materialized view metadata   
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{
    table_metadata::VersionNumber,
    view_metadata::{GeneralViewMetadata, Representation},
};

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<MaterializedViewRepresentation>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Fields for the version 2 of the view metadata.
pub enum MaterializedViewRepresentation {
    #[serde(rename_all = "kebab-case")]
    /// This type of representation stores the original view definition in SQL and its SQL dialect.
    SqlMaterialized {
        /// A string representing the original view definition in SQL
        sql: String,
        /// A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect.
        dialect: String,
        /// An integer version number for the materialized view format. Currently, this must be 1. Implementations must throw an exception if the materialized view's version is higher than the supported version.
        format_version: VersionNumber<1>,
        /// Pointer to the storage table
        storage_table: String,
    },
}

impl Representation for MaterializedViewRepresentation {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct Freshness {
    /// Version id of the materialized view when the refresh operation was performed.
    pub version_id: i64,
    /// Map from references in the sql expression to snapshot_ids of the last refresh operation
    pub base_tables: HashMap<String, i64>,
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::spec::materialized_view_metadata::MaterializedViewMetadata;

    #[test]
    fn test_deserialize_materialized_view_metadata_v1() -> Result<()> {
        let data = r#"
        {
        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
        "format-version" : 1,
        "location" : "s3://bucket/warehouse/default.db/event_agg",
        "current-version-id" : 1,
        "properties" : {
            "comment" : "Daily event counts"
        },
        "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
            "operation" : "create",
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
            },
            "representations" : [ {
            "type" : "sql-materialized",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark",
            "format-version": 1,
            "storage-table": "s3://bucket/warehouse/default.db/event_agg/computed"
            } ]
        } ],
        "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
            }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
            } ]
        } ],
        "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
        } ]
        }
        "#;
        let metadata = serde_json::from_str::<MaterializedViewMetadata>(data)
            .expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: MaterializedViewMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        dbg!(&metadata, &metadata_two);
        assert_eq!(metadata, metadata_two);

        Ok(())
    }
}
