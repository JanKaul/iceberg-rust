/*!
 * A Struct for the materialized view metadata   
*/

use serde::{Deserialize, Serialize};

use super::{
    table_metadata::VersionNumber,
    view_metadata::{GeneralViewMetadata, GeneralViewMetadataBuilder},
};

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<String>;
/// Builder for materialized view metadata
pub type MaterializedViewMetadataBuilder = GeneralViewMetadataBuilder<String>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "FormatVersionSerde", into = "FormatVersionSerde")]
/// Format version of the materialized view
pub enum FormatVersion {
    /// Version 1
    V1,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
/// Format version of the materialized view
pub enum FormatVersionSerde {
    /// Version 1
    V1(VersionNumber<1>),
}

impl From<FormatVersion> for FormatVersionSerde {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => FormatVersionSerde::V1(VersionNumber::<1>),
        }
    }
}

impl From<FormatVersionSerde> for FormatVersion {
    fn from(value: FormatVersionSerde) -> Self {
        match value {
            FormatVersionSerde::V1(_) => FormatVersion::V1,
        }
    }
}

/// Version id of the materialized view when the refresh operation was performed.
pub type VersionId = i64;
/// Map from references in the sql expression to snapshot_ids of the last refresh operation
pub type BaseTables = Vec<BaseTable>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct BaseTable {
    /// Table reference in the SQL expression.
    pub identifier: String,
    /// Snapshot id of the base table when the refresh operation was performed.
    pub snapshot_id: i64,
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
        } ],
        "materialization": "s3://bucket/path/to/metadata.json"
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
