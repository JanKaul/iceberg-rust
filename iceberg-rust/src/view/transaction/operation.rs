/*!
 * Defines the different [Operation]s on a [View].
*/

use iceberg_rust_spec::spec::{
    schema::Schema,
    table_metadata::MAIN_BRANCH,
    types::StructType,
    view_metadata::{
        GeneralViewMetadata, Operation as SummaryOperation, Summary, Version, ViewRepresentation,
        REF_PREFIX,
    },
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;

/// View operation
pub enum Operation {
    /// Update vresion
    UpdateRepresentation {
        /// Representation to add
        representation: ViewRepresentation,
        /// Schema of the representation
        schema: StructType,
        /// Branch where to add the representation
        branch: Option<String>,
    },
    /// Update view properties
    UpdateProperties(Vec<(String, String)>),
}

impl Operation {
    /// Execute operation
    pub async fn execute<T: Clone>(
        self,
        metadata: &mut GeneralViewMetadata<T>,
    ) -> Result<(), Error> {
        match self {
            Operation::UpdateRepresentation {
                representation,
                schema,
                branch,
            } => {
                let version_id = metadata.versions.keys().max().unwrap_or(&0) + 1;
                let schema_id = metadata.schemas.keys().max().unwrap_or(&0) + 1;
                metadata.schemas.insert(
                    schema_id,
                    Schema {
                        schema_id,
                        identifier_field_ids: None,
                        fields: schema,
                    },
                );
                let version = Version {
                    version_id,
                    schema_id,
                    summary: Summary {
                        operation: SummaryOperation::Replace,
                        engine_name: None,
                        engine_version: None,
                    },
                    representations: vec![representation],
                    default_catalog: None,
                    default_namespace: None,
                    timestamp_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as i64,
                };
                metadata.versions.insert(version_id, version);

                let branch_name = branch.unwrap_or("main".to_string());
                if branch_name == MAIN_BRANCH {
                    metadata.current_version_id = version_id;
                }
                metadata.properties.insert(
                    REF_PREFIX.to_string() + &branch_name,
                    version_id.to_string(),
                );

                Ok(())
            }
            Operation::UpdateProperties(entries) => {
                let properties = &mut metadata.properties;
                entries.into_iter().for_each(|(key, value)| {
                    properties.insert(key, value);
                });
                Ok(())
            }
        }
    }
}
