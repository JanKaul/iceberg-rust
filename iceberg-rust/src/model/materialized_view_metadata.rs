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
    #[serde(rename_all = "kebab-case")]
    /// This type of representation stores the original view definition in SQL and its SQL dialect.
    SqlMaterialized {
        /// A string representing the original view definition in SQL
        sql: String,
        /// An integer version number for the materialized view format. Currently, this must be 1. Implementations must throw an exception if the materialized view's version is higher than the supported version.
        format_version: VersionNumber<1>,
        /// A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect.
        dialect: String,
        /// Pointer to the storage table
        storage_table: String,
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

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::model::materialized_view_metadata::MaterializedViewMetadata;

    #[test]
    fn test_deserialize_materialized_view_metadata_v1() -> Result<()> {
        let data = r#"
        {
            "format-version" : 1,
            "location" : "s3n://my_company/my/warehouse/anorwood.db/common_view",
            "current-version-id" : 1,
            "properties" : { 
              "comment" : "View captures all the data from the table"
            },
            "versions" : [ {
              "version-id" : 1,
              "parent-version-id" : -1,
              "timestamp-ms" : 1573518431292,
              "summary" : {
                "operation" : "create",
                "engineVersion" : "presto-350"
              },
              "representations" : [ {
                "type" : "sql-materialized",
                "sql" : "SELECT *\nFROM\n  base_tab\n",
                "format-version": 1,
                "storage-table": "s3n://my_company/my/warehouse/anorwood.db/storage_table",
                "dialect" : "presto",
                "schema-id" : 1,
                "default-catalog" : "iceberg",
                "default-namespace" : [ "anorwood" ]
              } ]
            } ],
            "version-log" : [ {
              "timestamp-ms" : 1573518431292,
              "version-id" : 1
            } ],
            "schemas": [ {
              "schema-id": 1,
              "type" : "struct",
              "fields" : [ {
                "id" : 0,
                "name" : "c1",
                "required" : false,
                "type" : "int",
                "doc" : ""
              }, {
                "id" : 1,
                "name" : "c2",
                "required" : false,
                "type" : "string",
                "doc" : ""
              } ]
            } ],
            "current-schema-id": 1
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
