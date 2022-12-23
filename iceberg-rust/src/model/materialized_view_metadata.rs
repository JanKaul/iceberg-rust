/*!
 * Materialized Views
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Part of the metadata of an iceberg materialized view that is stored inside the view's properties field.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "format-version")]
pub enum MaterializedViewMetadataInView {
    /// Version 1 of the materialized view metadata
    #[serde(rename = "1")]
    V1(MaterializedViewMetadataInViewV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 1 of the view metadata.
pub struct MaterializedViewMetadataInViewV1 {
    /// Path to the metadata file of the storage table.
    pub storage_table_location: String,
    /// Boolean that defines the query engine behavior in case the base tables indicate the precomputed data isn't fresh.
    /// If set to FALSE, a refresh operation has to be performed before the query results are returned.
    /// If set to TRUE the data in the storage table gets returned without performing a refresh operation. If field is not set, defaults to FALSE.
    pub allow_stale_data: Option<bool>,
    /// How the storage table is supposed to be refreshed. If field is not set, defaults to full
    pub refresh_strategy: Option<RefreshStrategy>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// Refresh strategies
pub enum RefreshStrategy {
    /// Full storage table refresh
    Full,
    /// Incremental table refresh
    Incremental,
}

/// Part of the metadata of an iceberg materialized view that is stored inside the storage table's properties field.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "format-version")]
pub enum MaterializedViewMetadataInTable {
    /// Version 1 of the materialized view metadata
    #[serde(rename = "1")]
    V1(MaterializedViewMetadataInTableV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 1 of the view metadata.
pub struct MaterializedViewMetadataInTableV1 {
    /// A list of refresh operations.
    pub refreshes: Vec<RefreshOperation>,
    /// Id of the last refresh operation that defines the current state of the data files.
    pub current_refresh_id: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Table refrence to an iceberg table in a metastore
pub struct RefreshOperation {
    /// ID of the refresh operation.
    pub refresh_id: i64,
    /// Version id of the materialized view when the refresh operation was performed.
    pub version_id: i64,
    /// A List of base-table records.
    pub base_tables: Vec<BaseTable>,
    /// Sequence number of the snapshot that contains the refreshed data files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
}

/// A base table reference
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BaseTable {
    /// An iceberg metastore table
    IcebergMetastore(IcebergMetastore),
    /// An iceberg filesystem table
    IcebergFilesystem(IcebergFilesystem),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Table refrence to an iceberg table in a metastore
pub struct IcebergMetastore {
    /// Identifier in the SQL expression.
    pub identifier: String,
    /// Snapshot id of the base table when the refresh operation was performed.
    pub snapshot_reference: i64,
    /// A string to string map of base table properties. Could be used to specify a different metastore.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Table refrence to an iceberg table in a metastore
pub struct IcebergFilesystem {
    /// Identifier in the SQL expression.
    pub identifier: String,
    /// Path to the directory of the base table.
    pub location: String,
    /// Snapshot id of the base table when the refresh operation was performed.
    pub snapshot_reference: i64,
    /// A string to string map of base table properties. Could be used to specify a different metastore.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_in_view() {
        let metadata = r#"
        {
            "format-version": "1",
            "storage-table-location": "s3n://my_company/my/warehouse/anorwood.db/materialized_view/storage",
            "allow-stale-data": true,
            "refresh-strategy": "incremental"
        }
        "#;

        let result: MaterializedViewMetadataInView = serde_json::from_str(&metadata).unwrap();
        if let MaterializedViewMetadataInView::V1(result) = &result {
            assert_eq!(
                result.storage_table_location,
                "s3n://my_company/my/warehouse/anorwood.db/materialized_view/storage"
            );
            assert_eq!(result.allow_stale_data, Some(true));
            assert_eq!(result.refresh_strategy, Some(RefreshStrategy::Incremental));
        } else {
            panic!()
        }

        let result_two: MaterializedViewMetadataInView = serde_json::from_str(
            &serde_json::to_string(&result).expect("Failed to serialize result"),
        )
        .expect("Failed to serialize json");
        assert_eq!(result, result_two);
    }

    #[test]
    fn metadata_in_table() {
        let metadata = r#"
        {
            "format-version": "1",
            "refreshes": [
                {
                    "refresh-id": 1,
                    "version-id": 2,
                    "base-tables": [
                        {
                            "type": "iceberg-metastore",
                            "identifier": "public.nyc_taxis",
                            "snapshot-reference": 3453566763342345367
                        }
                    ] 
                }
            ],
            "current-refresh-id": 1
        }
        "#;

        let result: MaterializedViewMetadataInTable = serde_json::from_str(&metadata).unwrap();
        if let MaterializedViewMetadataInTable::V1(result) = &result {
            assert_eq!(
                result.refreshes[0],
                RefreshOperation {
                    refresh_id: 1,
                    version_id: 2,
                    base_tables: vec![BaseTable::IcebergMetastore(IcebergMetastore {
                        identifier: "public.nyc_taxis".to_owned(),
                        snapshot_reference: 3453566763342345367,
                        properties: None,
                    })],
                    sequence_number: None,
                }
            );
            assert_eq!(result.current_refresh_id, 1);
        } else {
            panic!()
        }

        let result_two: MaterializedViewMetadataInTable = serde_json::from_str(
            &serde_json::to_string(&result).expect("Failed to serialize result"),
        )
        .expect("Failed to serialize json");
        assert_eq!(result, result_two);
    }
}
