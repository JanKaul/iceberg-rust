//! Materialized view metadata types and functionality
//!
//! This module contains the types and implementations for managing materialized view metadata in Apache Iceberg.
//! It includes structures for tracking view states, source tables, and refresh operations.
//!
//! The main types are:
//! - [`MaterializedViewMetadata`]: The top-level metadata for a materialized view
//! - [`RefreshState`]: Information about the last refresh operation
//! - [`SourceTables`]: Collection of source table states
//! - [`SourceViews`]: Collection of source view states

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identifier::FullIdentifier;

use super::{
    tabular::TabularMetadataRef,
    view_metadata::{GeneralViewMetadata, GeneralViewMetadataBuilder},
};

pub static REFRESH_STATE: &str = "refresh-state";

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<FullIdentifier>;
/// Builder for materialized view metadata
pub type MaterializedViewMetadataBuilder = GeneralViewMetadataBuilder<FullIdentifier>;

impl MaterializedViewMetadata {
    pub fn as_ref(&self) -> TabularMetadataRef<'_> {
        TabularMetadataRef::MaterializedView(self)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Freshness information of the materialized view
pub struct RefreshState {
    /// The version-id of the materialized view when the refresh operation was performed.
    pub refresh_version_id: i64,
    /// A map from sequence-id (as defined in the view lineage) to the source tables’ snapshot-id of when the last refresh operation was performed.
    pub source_table_states: SourceTables,
    /// A map from sequence-id (as defined in the view lineage) to the source views’ version-id of when the last refresh operation was performed.
    pub source_view_states: SourceViews,
}

/// Represents a collection of source table states in a materialized view refresh
///
/// # Fields
/// * `0` - A HashMap mapping (table UUID, optional reference) pairs to snapshot IDs
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "Vec<SourceTable>", into = "Vec<SourceTable>")]
pub struct SourceTables(pub HashMap<(Uuid, Option<String>), i64>);

/// Represents a collection of source view states in a materialized view refresh
///
/// # Fields
/// * `0` - A HashMap mapping (table UUID, optional reference) pairs to version IDs
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "Vec<SourceView>", into = "Vec<SourceView>")]
pub struct SourceViews(pub HashMap<(Uuid, Option<String>), i64>);

/// Represents a source table state in a materialized view refresh
///
/// # Fields
/// * `uuid` - The UUID of the source table
/// * `snapshot_id` - The snapshot ID of the source table at refresh time
/// * `ref` - Optional reference name (e.g. branch or tag) used to access the source table
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SourceTable {
    uuid: Uuid,
    snapshot_id: i64,
    r#ref: Option<String>,
}

/// Represents a source view state in a materialized view refresh
///
/// # Fields
/// * `uuid` - The UUID of the source view
/// * `version_id` - The version ID of the source view at refresh time
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SourceView {
    uuid: Uuid,
    version_id: i64,
}

impl From<Vec<SourceTable>> for SourceTables {
    fn from(value: Vec<SourceTable>) -> Self {
        SourceTables(
            value
                .into_iter()
                .map(|x| ((x.uuid, x.r#ref), x.snapshot_id))
                .collect(),
        )
    }
}

impl From<SourceTables> for Vec<SourceTable> {
    fn from(value: SourceTables) -> Self {
        value
            .0
            .into_iter()
            .map(|((uuid, r#ref), snapshot_id)| SourceTable {
                uuid,
                snapshot_id,
                r#ref,
            })
            .collect()
    }
}

impl From<Vec<SourceView>> for SourceViews {
    fn from(value: Vec<SourceView>) -> Self {
        SourceViews(
            value
                .into_iter()
                .map(|x| ((x.uuid, None), x.version_id))
                .collect(),
        )
    }
}

impl From<SourceViews> for Vec<SourceView> {
    fn from(value: SourceViews) -> Self {
        value
            .0
            .into_iter()
            .map(|((uuid, _), version_id)| SourceView { uuid, version_id })
            .collect()
    }
}

impl Deref for SourceTables {
    type Target = HashMap<(Uuid, Option<String>), i64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SourceViews {
    type Target = HashMap<(Uuid, Option<String>), i64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
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
