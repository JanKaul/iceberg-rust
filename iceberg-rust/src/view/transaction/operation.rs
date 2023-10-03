/*!
 * Defines the different [Operation]s on a [View].
*/

use std::collections::HashMap;

use anyhow::Result;

use crate::{model::schema::Schema, view::View};

/// View operation
pub enum Operation {
    /// Update schema
    UpdateSchema(Schema),
    // /// Update table properties
    // UpdateProperties,
    /// Update the table location
    UpdateLocation(String),
}

impl Operation {
    /// Execute operation
    pub async fn execute(self, view: &mut View) -> Result<()> {
        match self {
            Operation::UpdateLocation(location) => {
                view.metadata_location = location;
                Ok(())
            }
            Operation::UpdateSchema(schema) => match &mut view.metadata.schemas {
                None => {
                    view.metadata.current_schema_id = Some(schema.schema_id);
                    view.metadata.schemas =
                        Some(HashMap::from_iter(vec![(schema.schema_id, schema)]));
                    Ok(())
                }
                Some(schemas) => {
                    view.metadata.current_schema_id = Some(schema.schema_id);
                    schemas.insert(schema.schema_id, schema);
                    Ok(())
                }
            },
        }
    }
}
