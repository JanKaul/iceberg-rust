//! Materialized view metadata types and functionality
//!
//! This module contains the types and implementations for managing materialized view metadata in Apache Iceberg.
//! It includes structures for tracking view states, source tables, and refresh operations.
//!
//! The main types are:
//! - [`MaterializedViewMetadata`]: The top-level metadata for a materialized view
//! - [`RefreshState`]: Information about the last refresh operation
//! - [`SourceStates`]: Collection of source states

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    identifier::{FullIdentifier, Identifier},
    namespace::Namespace,
};

use super::{
    tabular::TabularMetadataRef,
    view_metadata::{GeneralViewMetadata, GeneralViewMetadataBuilder},
};

pub static REFRESH_STATE: &str = "refresh-state";

/// Fields for the version 1 of the view metadata.
pub type MaterializedViewMetadata = GeneralViewMetadata<Identifier>;
/// Builder for materialized view metadata
pub type MaterializedViewMetadataBuilder = GeneralViewMetadataBuilder<Identifier>;

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
    pub source_states: SourceStates,
    // A timestamp of when the refresh operation was started
    pub refresh_start_timestamp_ms: i64,
}

/// Represents a collection of source view states in a materialized view refresh
///
/// # Fields
/// * `0` - A HashMap mapping (table UUID, optional reference) pairs to version IDs
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "Vec<SourceState>", into = "Vec<SourceState>")]
pub struct SourceStates(pub HashMap<FullIdentifier, SourceState>);

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum SourceState {
    Table(SourceTable),
    View(SourceView),
}

/// Represents a source table state in a materialized view refresh
///
/// # Fields
/// * `uuid` - The UUID of the source table
/// * `snapshot_id` - The snapshot ID of the source table at refresh time
/// * `ref` - Optional reference name (e.g. branch or tag) used to access the source table
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SourceTable {
    name: String,
    namespace: Namespace,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    uuid: Uuid,
    snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    name: String,
    namespace: Namespace,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    uuid: Uuid,
    version_id: i64,
}

impl SourceTable {
    /// Create a new SourceTable
    pub fn new(
        catalog: Option<&str>,
        namespace: &[String],
        name: &str,
        uuid: Uuid,
        snapshot_id: i64,
        r#ref: Option<String>,
    ) -> Self {
        Self {
            catalog: catalog.map(ToString::to_string),
            namespace: Namespace(namespace.to_owned()),
            name: name.to_owned(),
            uuid,
            snapshot_id,
            r#ref,
        }
    }
}

impl SourceState {
    /// Returns the snapshot_id for Table states, None for View states
    pub fn snapshot_id(&self) -> Option<i64> {
        match self {
            SourceState::Table(t) => Some(t.snapshot_id),
            SourceState::View(_) => None,
        }
    }
}

impl From<Vec<SourceState>> for SourceStates {
    fn from(value: Vec<SourceState>) -> Self {
        SourceStates(
            value
                .into_iter()
                .map(|x| match &x {
                    SourceState::Table(table) => (
                        FullIdentifier::new(
                            table.catalog.as_deref(),
                            &table.namespace,
                            &table.name,
                        ),
                        x,
                    ),
                    SourceState::View(view) => (
                        FullIdentifier::new(view.catalog.as_deref(), &view.namespace, &view.name),
                        x,
                    ),
                })
                .collect(),
        )
    }
}

impl From<SourceStates> for Vec<SourceState> {
    fn from(value: SourceStates) -> Self {
        value.0.into_values().collect()
    }
}

impl Deref for SourceStates {
    type Target = HashMap<FullIdentifier, SourceState>;
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
