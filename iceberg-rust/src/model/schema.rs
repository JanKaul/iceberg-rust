use serde::{Deserialize, Serialize};

use super::data_types::Struct as StructType;

/**
 * Schemas
*/

/// Schema of an iceberg table
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Schema {
    /// Version 2 of the table schema
    V2(SchemaV2),
    /// Version 1 of the table schema
    V1(SchemaV1),
}

impl Schema {
    /// Struct fields of the schema
    pub fn fields(&self) -> &StructType {
        match self {
            Schema::V2(schema) => &schema.fields,
            Schema::V1(schema) => &schema.fields,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "struct", rename_all = "kebab-case", tag = "type")]
/// Names and types of fields in a table.
pub struct SchemaV2 {
    /// Identifier of the schema
    pub schema_id: i32,
    /// Set of primitive fields that identify rows in a table.
    pub identifier_field_ids: Option<Vec<i32>>,

    #[serde(flatten)]
    /// The struct fields
    pub fields: StructType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "struct", rename_all = "kebab-case", tag = "type")]
/// Names and types of fields in a table.
pub struct SchemaV1 {
    /// Identifier of the schema
    pub schema_id: Option<i32>,
    /// Set of primitive fields that identify rows in a table.
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
    use crate::model::data_types::{PrimitiveType, Type};

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

        let result: SchemaV2 = serde_json::from_str(&record).unwrap();
        assert_eq!(1, result.schema_id);
        assert_eq!(
            Type::Primitive(PrimitiveType::Uuid),
            result.fields[0].field_type
        );
        assert_eq!(1, result.fields[0].id);
        assert_eq!(true, result.fields[0].required);

        assert_eq!(
            Type::Primitive(PrimitiveType::Int),
            result.fields[1].field_type
        );
        assert_eq!(2, result.fields[1].id);
        assert_eq!(false, result.fields[1].required);
    }
}
