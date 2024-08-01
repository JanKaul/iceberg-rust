/*!
 * A Struct for the materialized view metadata   
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::view_metadata::FullIdentifier;

use super::view_metadata::{GeneralViewMetadata, GeneralViewMetadataBuilder};

pub static REFRESH_STATE: &str = "refresh-state";

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<FullIdentifier>;
/// Builder for materialized view metadata
pub type MaterializedViewMetadataBuilder = GeneralViewMetadataBuilder<FullIdentifier>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct RefreshState {
    /// The version-id of the materialized view when the refresh operation was performed.
    pub refresh_version_id: i64,
    /// A map from sequence-id (as defined in the view lineage) to the source tables’ snapshot-id of when the last refresh operation was performed.
    pub source_table_states: HashMap<String, i64>,
    /// A map from sequence-id (as defined in the view lineage) to the source views’ version-id of when the last refresh operation was performed.
    pub source_view_states: HashMap<String, i64>,
}

#[cfg(test)]
mod tests {

    use crate::{error::Error, spec::materialized_view_metadata::MaterializedViewMetadata};

    #[test]
    fn test_deserialize_materialized_view_metadata_v1() -> Result<(), Error> {
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
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
            } ],
            "storage-table": {
                "catalog": "prod",
                "namespace": ["default"],
                "name": "event_agg_storage"
            }
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
        assert_eq!(metadata, metadata_two);

        Ok(())
    }
}
