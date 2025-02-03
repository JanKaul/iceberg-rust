/*!
 * Schema definition and management for Iceberg tables
 *
 * This module provides the core schema functionality for Iceberg tables, including:
 * - Schema versioning and evolution
 * - Field definitions with unique IDs
 * - Required vs optional field specifications
 * - Schema builder patterns for constructing complex schemas
 * - Schema projection for selecting subsets of fields
 *
 * The schema system is fundamental to Iceberg's data model, providing:
 * - Type safety and validation
 * - Schema evolution capabilities
 * - Efficient field access via ID-based lookups
 * - Support for nested data structures
 */

use std::{fmt, ops::Deref, str};

use super::types::{StructField, StructType, StructTypeBuilder};
use derive_getters::Getters;
use serde::{Deserialize, Serialize};

use crate::error::Error;

pub static DEFAULT_SCHEMA_ID: i32 = 0;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Getters)]
#[serde(rename_all = "kebab-case")]
/// Names and types of fields in a table.
pub struct Schema {
    /// Identifier of the schema
    schema_id: i32,
    /// Set of primitive fields that identify rows in a table.
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<i32>>,

    #[serde(flatten)]
    /// The struct fields
    fields: StructType,
}

impl Deref for Schema {
    type Target = StructType;
    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl Schema {
    /// Creates a new SchemaBuilder to construct a Schema using the builder pattern
    ///
    /// # Returns
    /// * A SchemaBuilder instance configured with default values
    ///
    /// This is the recommended way to construct Schema instances when you need
    /// to add fields incrementally or set optional parameters.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// Creates a new Schema from a StructType and associated metadata
    ///
    /// # Arguments
    /// * `fields` - The StructType containing the schema's fields
    /// * `schema_id` - Unique identifier for this schema
    /// * `identifier_field_ids` - Optional list of field IDs that identify rows in the table
    ///
    /// # Returns
    /// * A new Schema instance with the provided fields and metadata
    pub fn from_struct_type(
        fields: StructType,
        schema_id: i32,
        identifier_field_ids: Option<Vec<i32>>,
    ) -> Self {
        Schema {
            schema_id,
            identifier_field_ids,
            fields,
        }
    }

    /// Creates a new Schema containing only the specified field IDs
    ///
    /// # Arguments
    /// * `ids` - Array of field IDs to include in the projected schema
    ///
    /// # Returns
    /// * A new Schema containing only the specified fields, maintaining the original
    ///   schema ID and any identifier fields that were included in the projection
    pub fn project(&self, ids: &[i32]) -> Schema {
        Schema {
            schema_id: self.schema_id,
            identifier_field_ids: self.identifier_field_ids.as_ref().map(|x| {
                x.iter()
                    .filter(|x| ids.contains(x))
                    .map(ToOwned::to_owned)
                    .collect()
            }),
            fields: StructType::new(
                self.fields()
                    .iter()
                    .filter(|x| ids.contains(&x.id))
                    .map(ToOwned::to_owned)
                    .collect(),
            ),
        }
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for Schema {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

#[derive(Default)]
pub struct SchemaBuilder {
    schema_id: Option<i32>,
    identifier_field_ids: Option<Vec<i32>>,
    fields: StructTypeBuilder,
}

impl SchemaBuilder {
    /// Sets the schema ID for this schema
    ///
    /// # Arguments
    /// * `schema_id` - The unique identifier for this schema
    ///
    /// # Returns
    /// * A mutable reference to self for method chaining
    pub fn with_schema_id(&mut self, schema_id: i32) -> &mut Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Sets the identifier field IDs for this schema
    ///
    /// # Arguments
    /// * `ids` - Collection of field IDs that identify rows in the table
    ///
    /// # Returns
    /// * A mutable reference to self for method chaining
    pub fn with_identifier_field_ids(&mut self, ids: impl Into<Vec<i32>>) -> &mut Self {
        self.identifier_field_ids = Some(ids.into());
        self
    }

    /// Adds a struct field to this schema
    ///
    /// # Arguments
    /// * `field` - The StructField to add to the schema
    ///
    /// # Returns
    /// * A mutable reference to self for method chaining
    pub fn with_struct_field(&mut self, field: StructField) -> &mut Self {
        self.fields.with_struct_field(field);
        self
    }

    /// Builds and returns a new Schema from this builder's configuration
    ///
    /// # Returns
    /// * `Ok(Schema)` - A new Schema instance with the configured fields and metadata
    /// * `Err(Error)` - If there was an error building the schema
    pub fn build(&mut self) -> Result<Schema, Error> {
        let fields = self.fields.build()?;

        Ok(Schema {
            schema_id: self.schema_id.unwrap_or(DEFAULT_SCHEMA_ID),
            identifier_field_ids: self.identifier_field_ids.take(),
            fields,
        })
    }
}

impl TryFrom<SchemaV2> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV2) -> Result<Self, Self::Error> {
        Ok(Schema {
            schema_id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            fields: value.fields,
        })
    }
}

impl TryFrom<SchemaV1> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV1) -> Result<Self, Self::Error> {
        Ok(Schema {
            schema_id: value.schema_id.unwrap_or(0),
            identifier_field_ids: value.identifier_field_ids,
            fields: value.fields,
        })
    }
}

impl From<Schema> for SchemaV2 {
    fn from(value: Schema) -> Self {
        SchemaV2 {
            schema_id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            fields: value.fields,
        }
    }
}

impl From<Schema> for SchemaV1 {
    fn from(value: Schema) -> Self {
        SchemaV1 {
            schema_id: Some(value.schema_id),
            identifier_field_ids: value.identifier_field_ids,
            fields: value.fields,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Names and types of fields in a table.
pub struct SchemaV2 {
    /// Identifier of the schema
    pub schema_id: i32,
    /// Set of primitive fields that identify rows in a table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,

    #[serde(flatten)]
    /// The struct fields
    pub fields: StructType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Names and types of fields in a table.
pub struct SchemaV1 {
    /// Identifier of the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// Set of primitive fields that identify rows in a table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,

    #[serde(flatten)]
    /// The struct fields
    pub fields: StructType,
}

impl From<SchemaV1> for SchemaV2 {
    fn from(v1: SchemaV1) -> Self {
        SchemaV2 {
            schema_id: v1.schema_id.unwrap_or(0),
            identifier_field_ids: v1.identifier_field_ids,
            fields: v1.fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::types::{PrimitiveType, Type};

    use super::*;

    #[test]
    fn schema() {
        let record = r#"
        {
            "type": "struct",
            "schema-id": 1,
            "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid"
            }, {
            "id": 2,
            "name": "data",
            "required": false,
            "type": "int"
            } ]
            }
        "#;

        let result: SchemaV2 = serde_json::from_str(record).unwrap();
        assert_eq!(1, result.schema_id);
        assert_eq!(
            Type::Primitive(PrimitiveType::Uuid),
            result.fields[0].field_type
        );
        assert_eq!(1, result.fields[0].id);
        assert!(result.fields[0].required);

        assert_eq!(
            Type::Primitive(PrimitiveType::Int),
            result.fields[1].field_type
        );
        assert_eq!(2, result.fields[1].id);
        assert!(!result.fields[1].required);
    }
}
