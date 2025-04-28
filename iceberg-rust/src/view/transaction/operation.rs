/*!
 * Defines the different [Operation]s on a [View].
*/

use iceberg_rust_spec::{
    schema::Schema,
    spec::{
        types::StructType,
        view_metadata::{GeneralViewMetadata, Summary, Version, ViewRepresentation, REF_PREFIX},
    },
    view_metadata::Materialization,
};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    catalog::commit::{ViewRequirement, ViewUpdate},
    error::Error,
};

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
    pub async fn execute<T: Materialization>(
        self,
        metadata: &GeneralViewMetadata<T>,
    ) -> Result<(Option<ViewRequirement>, Vec<ViewUpdate<T>>), Error> {
        match self {
            Operation::UpdateRepresentation {
                representation,
                schema,
                branch,
            } => {
                let schema_changed = metadata.current_schema(branch.as_deref())
                    .map(|s| schema != *s.fields())
                    .unwrap_or(true);

                let version = metadata.current_version(branch.as_deref())?;
                let version_id = metadata.versions.keys().max().unwrap_or(&0) + 1;
                let schema_id = if schema_changed {
                    metadata.schemas.keys().max().unwrap_or(&0) + 1
                } else {
                    *metadata.current_schema(branch.as_deref()).unwrap().schema_id()
                };
                let last_column_id = schema.iter().map(|x| x.id).max().unwrap_or(0);

                let version = Version {
                    version_id,
                    schema_id,
                    summary: Summary {
                        operation: iceberg_rust_spec::spec::view_metadata::Operation::Replace,
                        engine_name: None,
                        engine_version: None,
                    },
                    representations: vec![representation],
                    default_catalog: version.default_catalog.clone(),
                    default_namespace: version.default_namespace.clone(),
                    timestamp_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as i64,
                    storage_table: version.storage_table.clone(),
                };

                let branch_name = branch.unwrap_or("main".to_string());

                let mut view_updates: Vec<ViewUpdate<T>> = if schema_changed {
                    vec![ViewUpdate::AddSchema {
                        schema: Schema::from_struct_type(schema, schema_id, None),
                        last_column_id: Some(last_column_id),
                    }]
                } else {
                    vec![]
                };

                view_updates.append(&mut vec![
                    ViewUpdate::AddViewVersion {
                        view_version: version,
                    },
                    ViewUpdate::SetCurrentViewVersion {
                        view_version_id: version_id,
                    },
                    ViewUpdate::SetProperties {
                        updates: HashMap::from_iter(vec![(
                            REF_PREFIX.to_string() + &branch_name,
                            version_id.to_string(),
                        )]),
                    },
                ]);

                Ok((
                    Some(ViewRequirement::AssertViewUuid {
                        uuid: metadata.view_uuid,
                    }),
                    view_updates,
                ))
            }
            Operation::UpdateProperties(entries) => Ok((
                None,
                vec![ViewUpdate::SetProperties {
                    updates: HashMap::from_iter(entries),
                }],
            )),
        }
    }
}
