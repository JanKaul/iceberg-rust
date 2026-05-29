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

    // --- RefreshState / SourceState / SourceStates ----------------------

    use super::*;

    fn sample_source_table() -> SourceTable {
        SourceTable::new(
            Some("prod_cat"),
            &["analytics".to_string(), "events".to_string()],
            "page_views",
            Uuid::parse_str("aa000000-1111-2222-3333-444444444444").unwrap(),
            42,
            Some("staging".to_string()),
        )
    }

    fn sample_source_view() -> SourceView {
        SourceView {
            name: "session_facts".to_string(),
            namespace: Namespace(vec!["analytics".to_string(), "views".to_string()]),
            catalog: None,
            uuid: Uuid::parse_str("bb000000-1111-2222-3333-444444444444").unwrap(),
            version_id: 7,
        }
    }

    #[test]
    fn test_source_table_new_constructs_expected_struct() {
        let t = sample_source_table();
        // Re-serialize and inspect the JSON to confirm field shapes
        // including the kebab-case rename of `snapshot-id` and `ref`.
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"name\":\"page_views\""));
        assert!(json.contains("\"namespace\":[\"analytics\",\"events\"]"));
        assert!(json.contains("\"catalog\":\"prod_cat\""));
        assert!(json.contains("\"snapshot-id\":42"));
        assert!(json.contains("\"ref\":\"staging\""));
    }

    #[test]
    fn test_source_state_table_round_trip() {
        let state = SourceState::Table(sample_source_table());
        let json = serde_json::to_string(&state).unwrap();
        // The `tag = "type"` attribute injects the discriminator.
        assert!(json.contains("\"type\":\"table\""));
        let parsed: SourceState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, state);
    }

    #[test]
    fn test_source_state_view_round_trip() {
        let state = SourceState::View(sample_source_view());
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"type\":\"view\""));
        let parsed: SourceState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, state);
    }

    #[test]
    fn test_source_state_snapshot_id_returns_some_for_table_and_none_for_view() {
        assert_eq!(SourceState::Table(sample_source_table()).snapshot_id(), Some(42));
        assert_eq!(SourceState::View(sample_source_view()).snapshot_id(), None);
    }

    #[test]
    fn test_source_states_round_trip_via_vec_representation() {
        // Two source states keyed by their FullIdentifiers; the type
        // serializes as a JSON array via the `from`/`into` Vec adapters.
        let states_vec = vec![
            SourceState::Table(sample_source_table()),
            SourceState::View(sample_source_view()),
        ];
        let source_states: SourceStates = states_vec.clone().into();
        assert_eq!(source_states.len(), 2);

        let json = serde_json::to_string(&source_states).unwrap();
        // The outer container is an array, not an object.
        assert!(json.starts_with('['), "expected JSON array, got {json}");

        let parsed: SourceStates = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        // The HashMap re-keys by FullIdentifier on deserialize; the keys
        // must match those produced from the original input.
        for state in &states_vec {
            let key = match state {
                SourceState::Table(t) => {
                    let json_value = serde_json::to_value(t).unwrap();
                    FullIdentifier::new(
                        json_value["catalog"].as_str(),
                        &Namespace(
                            json_value["namespace"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|v| v.as_str().unwrap().to_string())
                                .collect(),
                        ),
                        json_value["name"].as_str().unwrap(),
                    )
                }
                SourceState::View(v) => {
                    FullIdentifier::new(v.catalog.as_deref(), &v.namespace, &v.name)
                }
            };
            assert_eq!(parsed.get(&key), Some(state));
        }
    }

    #[test]
    fn test_refresh_state_round_trip_with_table_and_view_sources() {
        let refresh = RefreshState {
            refresh_version_id: 11,
            source_states: SourceStates::from(vec![
                SourceState::Table(sample_source_table()),
                SourceState::View(sample_source_view()),
            ]),
            refresh_start_timestamp_ms: 1_730_000_000_000,
        };
        let json = serde_json::to_string(&refresh).unwrap();
        // kebab-case rename on the outer struct.
        assert!(json.contains("\"refresh-version-id\":11"));
        assert!(json.contains("\"refresh-start-timestamp-ms\":1730000000000"));

        let parsed: RefreshState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, refresh);
    }
}
