/*!
 * Defines the different [Operation]s on a [View].
*/

use anyhow::Result;

use crate::{
    model::{schema::Schema, view_metadata::ViewMetadata},
    view::View,
};

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
            Operation::UpdateSchema(schema) => match &mut view.metadata {
                ViewMetadata::V1(metadata) => match &mut metadata.schemas {
                    None => {
                        metadata.current_schema_id = match &schema {
                            Schema::V1(schema) => schema.schema_id,
                            Schema::V2(schema) => Some(schema.schema_id),
                        };
                        metadata.schemas = Some(vec![schema]);
                        Ok(())
                    }
                    Some(schemas) => {
                        metadata.current_schema_id = match &schema {
                            Schema::V1(schema) => schema.schema_id,
                            Schema::V2(schema) => Some(schema.schema_id),
                        };
                        schemas.push(schema);
                        Ok(())
                    }
                },
            },
        }
    }
}
