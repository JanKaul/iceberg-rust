/*!
 * Defines the different [Operation]s on a [View].
*/

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::spec::{
    schema::Schema,
    view_metadata::{
        GeneralViewMetadata, Operation as SummaryOperation, Representation, Summary, Version,
    },
};

/// View operation
pub enum Operation<T: Representation> {
    /// Update vresion
    UpdateRepresentation {
        /// Representation to add
        representation: T,
        /// Schema of the representation
        schema: Schema,
    },
}

impl<T: Representation> Operation<T> {
    /// Execute operation
    pub async fn execute(self, metadata: &mut GeneralViewMetadata<T>) -> Result<()> {
        match self {
            Operation::UpdateRepresentation {
                representation,
                schema,
            } => {
                let new_version_number = metadata.versions.keys().max().unwrap_or(&0) + 1;
                let schema_id = schema.schema_id;
                metadata.add_schema(schema);
                let version = Version {
                    version_id: new_version_number,
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
                metadata.versions.insert(new_version_number, version);
                Ok(())
            }
        }
    }
}
