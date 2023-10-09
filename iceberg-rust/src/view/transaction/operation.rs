/*!
 * Defines the different [Operation]s on a [View].
*/

use std::collections::HashMap;

use anyhow::Result;

use crate::model::{
    schema::Schema,
    view_metadata::{GeneralViewMetadata, Representation},
};

/// View operation
pub enum Operation {
    /// Update schema
    UpdateSchema(Schema),
    // /// Update table properties
    // UpdateProperties,
}

impl Operation {
    /// Execute operation
    pub async fn execute<T: Representation>(
        self,
        metadata: &mut GeneralViewMetadata<T>,
    ) -> Result<()> {
        match self {
            Operation::UpdateSchema(schema) => match &mut metadata.schemas {
                None => {
                    metadata.schemas = Some(HashMap::from_iter(vec![(schema.schema_id, schema)]));
                    Ok(())
                }
                Some(schemas) => {
                    schemas.insert(schema.schema_id, schema);
                    Ok(())
                }
            },
        }
    }
}
