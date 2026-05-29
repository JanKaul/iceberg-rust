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
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{debug, instrument};

use crate::{
    catalog::commit::{ViewRequirement, ViewUpdate},
    error::Error,
};

#[derive(Debug)]
/// View operation
pub enum Operation {
    /// Update vresion
    UpdateRepresentations {
        /// Representation to add
        representations: Vec<ViewRepresentation>,
        /// Schema of the representation
        schema: StructType,
        /// Branch where to add the representation
        branch: Option<String>,
    },
    /// Update view properties
    UpdateProperties(Vec<(String, String)>),
}

// Tries to preserve dialect order
fn upsert_representation(
    current_representations: &[ViewRepresentation],
    new_representation: ViewRepresentation,
) -> Vec<ViewRepresentation> {
    let ViewRepresentation::Sql {
        dialect: new_dialect,
        ..
    } = &new_representation;
    let mut updated = false;
    let mut representations: Vec<ViewRepresentation> = current_representations
        .iter()
        .map(
            |current_representation @ ViewRepresentation::Sql { dialect, .. }| {
                if dialect == new_dialect {
                    updated = true;
                    new_representation.clone()
                } else {
                    current_representation.clone()
                }
            },
        )
        .collect();
    if !updated {
        representations.push(new_representation);
    }
    representations
}

fn upsert_representations(
    current_representations: &[ViewRepresentation],
    new_representations: &[ViewRepresentation],
) -> Vec<ViewRepresentation> {
    let mut representations: Vec<ViewRepresentation> = current_representations.into();
    for r in new_representations {
        representations = upsert_representation(&representations, r.clone());
    }
    representations
}

impl Operation {
    /// Execute operation
    #[instrument(
        name = "iceberg_rust::view::transaction::operation::execute",
        level = "debug"
    )]
    pub async fn execute<T: Materialization + Debug>(
        self,
        metadata: &GeneralViewMetadata<T>,
    ) -> Result<(Option<ViewRequirement>, Vec<ViewUpdate<T>>), Error> {
        match self {
            Operation::UpdateRepresentations {
                representations,
                schema,
                branch,
            } => {
                debug!(
                    "Executing UpdateRepresentations operation: representations={}, schema_fields={}, branch={:?}",
                    representations.len(),
                    schema.len(),
                    branch
                );
                let schema_changed = metadata
                    .current_schema(branch.as_deref())
                    .map(|s| schema != *s.fields())
                    .unwrap_or(true);

                let version = metadata.current_version(branch.as_deref())?;
                let version_id = metadata.versions.keys().max().unwrap_or(&0) + 1;
                let schema_id = if schema_changed {
                    metadata.schemas.keys().max().unwrap_or(&0) + 1
                } else {
                    *metadata
                        .current_schema(branch.as_deref())
                        .unwrap()
                        .schema_id()
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
                    representations: upsert_representations(
                        version.representations(),
                        &representations,
                    ),
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
            Operation::UpdateProperties(entries) => {
                debug!(
                    "Executing UpdateProperties operation: entries={:?}",
                    entries
                );
                Ok((
                    None,
                    vec![ViewUpdate::SetProperties {
                        updates: HashMap::from_iter(entries),
                    }],
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use iceberg_rust_spec::view_metadata::ViewRepresentation;

    use crate::view::transaction::operation::{upsert_representation, upsert_representations};

    fn sql(s: &str, dialect: &str) -> ViewRepresentation {
        ViewRepresentation::sql(s, Some(dialect))
    }

    // --- upsert_representation (singular) ----------------------------------
    //
    // The singular helper replaces an existing representation whose SQL
    // dialect matches the new one and otherwise appends to the end. The
    // dialect ordering of existing representations is preserved.

    #[test]
    fn test_upsert_representation_replaces_when_dialect_matches() {
        let current = vec![sql("a1", "a"), sql("b1", "b")];
        let updated = upsert_representation(&current, sql("a2", "a"));
        assert_eq!(updated, vec![sql("a2", "a"), sql("b1", "b")]);
    }

    #[test]
    fn test_upsert_representation_appends_when_dialect_is_new() {
        let current = vec![sql("a1", "a"), sql("b1", "b")];
        let updated = upsert_representation(&current, sql("c1", "c"));
        assert_eq!(
            updated,
            vec![sql("a1", "a"), sql("b1", "b"), sql("c1", "c")]
        );
    }

    #[test]
    fn test_upsert_representation_into_empty_list_appends_single_entry() {
        let updated = upsert_representation(&[], sql("only", "a"));
        assert_eq!(updated, vec![sql("only", "a")]);
    }

    #[test]
    fn test_upsert_representation_preserves_order_of_unmatched_dialects() {
        // Replacing the middle entry should leave the surrounding entries
        // in their original positions.
        let current = vec![sql("a1", "a"), sql("b1", "b"), sql("c1", "c")];
        let updated = upsert_representation(&current, sql("b2", "b"));
        assert_eq!(
            updated,
            vec![sql("a1", "a"), sql("b2", "b"), sql("c1", "c")]
        );
    }

    // --- upsert_representations (plural) -----------------------------------
    //
    // The plural helper folds the singular helper over a Vec of new
    // representations. New dialects are appended, matching dialects
    // replace the existing entry in place. The plural helper covers the
    // mixed case the original combined test exercised — split here into
    // focused assertions.

    #[test]
    fn test_upsert_representations_replaces_existing_dialect_and_appends_new() {
        let current = vec![sql("a1", "a"), sql("b1", "b")];
        let new = vec![sql("b2", "b"), sql("c2", "c")];
        assert_eq!(
            upsert_representations(&current, &new),
            vec![sql("a1", "a"), sql("b2", "b"), sql("c2", "c")],
        );
    }

    #[test]
    fn test_upsert_representations_processes_new_entries_in_order() {
        // The original combined test required both replace ("a") and
        // append ("c"); the new entries are supplied in order
        // (c-then-a). The plural helper folds them left-to-right, so
        // first "c" appends (a, b, c) and then "a" replaces position 0,
        // yielding (a2, b1, c2).
        let current = vec![sql("a1", "a"), sql("b1", "b")];
        let new = vec![sql("c2", "c"), sql("a2", "a")];
        assert_eq!(
            upsert_representations(&current, &new),
            vec![sql("a2", "a"), sql("b1", "b"), sql("c2", "c")],
        );
    }

    #[test]
    fn test_upsert_representations_empty_new_list_returns_current_unchanged() {
        let current = vec![sql("a1", "a"), sql("b1", "b")];
        assert_eq!(upsert_representations(&current, &[]), current);
    }
}
