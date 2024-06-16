/*!
 * A Struct for the materialized view metadata   
*/

use serde::{Deserialize, Serialize};

use crate::view_metadata::FullIdentifier;

use super::view_metadata::{GeneralViewMetadata, GeneralViewMetadataBuilder};

/// Property for the metadata location
pub static STORAGE_TABLE: &str = "storage_table";

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<FullIdentifier>;
/// Builder for materialized view metadata
pub type MaterializedViewMetadataBuilder = GeneralViewMetadataBuilder<FullIdentifier>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct RefreshTable {
    /// Sequence id in the materialized view lineage
    pub sequence_id: i64,
    /// Snapshot id of the base table when the refresh operation was performed.
    pub revision_id: i64,
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

    // #[test]
    // fn test_depends_on_tables_try_from_str() {
    //     let input = "table1=1,table2=2";

    //     let result = depends_on_tables_from_string(input).unwrap();

    //     assert_eq!(
    //         result,
    //         vec![
    //             RefreshTable {
    //                 identifier: "table1".to_string(),
    //                 revision_id: 1
    //             },
    //             RefreshTable {
    //                 identifier: "table2".to_string(),
    //                 revision_id: 2
    //             }
    //         ]
    //     );
    // }

    // #[test]
    // fn test_try_from_depends_on_tables_to_string() {
    //     let depends_on_tables = vec![
    //         RefreshTable {
    //             identifier: "table1".to_string(),
    //             revision_id: 1,
    //         },
    //         RefreshTable {
    //             identifier: "table2".to_string(),
    //             revision_id: 2,
    //         },
    //     ];

    //     let result = depends_on_tables_to_string(&depends_on_tables);

    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), "table1=1,table2=2");
    // }
}
