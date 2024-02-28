/*!
 * Defines the different [Operation]s on a [View].
*/

use iceberg_rust_spec::spec::{
    materialized_view_metadata::STORAGE_TABLE_LOCATION,
    schema::SchemaBuilder,
    types::StructType,
    view_metadata::{GeneralViewMetadata, Summary, Version, ViewRepresentation, REF_PREFIX},
};
use std::{
    any::Any,
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    catalog::commit::{ViewRequirement, ViewUpdate},
    error::Error,
};

/// View operation
pub enum Operation<T: Clone> {
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
    /// Update materialization
    UpdateMaterialization(T),
}

impl<T: Clone + Default + 'static> Operation<T> {
    /// Execute operation
    pub async fn execute(
        self,
        metadata: &GeneralViewMetadata<T>,
    ) -> Result<(Option<ViewRequirement>, Vec<ViewUpdate>), Error> {
        match self {
            Operation::UpdateRepresentation {
                representation,
                schema,
                branch,
            } => {
                let version_id = metadata.versions.keys().max().unwrap_or(&0) + 1;
                let schema_id = metadata.schemas.keys().max().unwrap_or(&0) + 1;
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
                    default_catalog: None,
                    default_namespace: None,
                    timestamp_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as i64,
                };

                let branch_name = branch.unwrap_or("main".to_string());

                Ok((
                    Some(ViewRequirement::AssertViewUuid {
                        uuid: metadata.view_uuid,
                    }),
                    vec![
                        ViewUpdate::AddViewVersion {
                            view_version: version,
                        },
                        ViewUpdate::SetCurrentViewVersion {
                            view_version_id: version_id,
                        },
                        ViewUpdate::AddSchema {
                            schema: SchemaBuilder::default()
                                .with_schema_id(schema_id)
                                .with_fields(schema)
                                .build()
                                .map_err(iceberg_rust_spec::error::Error::from)?,
                            last_column_id: Some(last_column_id),
                        },
                        ViewUpdate::SetProperties {
                            updates: HashMap::from_iter(vec![(
                                REF_PREFIX.to_string() + &branch_name,
                                version_id.to_string(),
                            )]),
                        },
                    ],
                ))
            }
            Operation::UpdateProperties(entries) => Ok((
                None,
                vec![ViewUpdate::SetProperties {
                    updates: HashMap::from_iter(entries),
                }],
            )),
            Operation::UpdateMaterialization(materialization) => {
                let previous_materialization =
                    (&metadata.properties.metadata_location as &dyn Any).downcast_ref::<String>();
                let materialization = (&materialization as &dyn Any)
                    .downcast_ref::<String>()
                    .ok_or(Error::InvalidFormat(
                        "Materialization is not a string.".to_owned(),
                    ))?;
                Ok((
                    previous_materialization.map(|x| ViewRequirement::AssertProperty {
                        property: (STORAGE_TABLE_LOCATION.to_string(), x.clone()),
                    }),
                    vec![ViewUpdate::SetProperties {
                        updates: HashMap::from_iter(vec![(
                            STORAGE_TABLE_LOCATION.to_string(),
                            materialization.to_string(),
                        )]),
                    }],
                ))
            }
        }
    }
}
