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
    use crate::spec::types::{ListType, MapType, PrimitiveType, StructField, Type};

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

    // --- Round-trip and behaviour ----------------------------------------

    fn primitive_field(id: i32, name: &str, required: bool, ty: PrimitiveType) -> StructField {
        StructField {
            id,
            name: name.to_string(),
            required,
            field_type: Type::Primitive(ty),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    #[test]
    fn test_schema_round_trip_preserves_doc_and_defaults() {
        let json = r#"{
            "type": "struct",
            "schema-id": 12,
            "fields": [
                { "id": 1, "name": "user_id",  "required": true,  "type": "long",   "doc": "primary identifier" },
                { "id": 2, "name": "username", "required": false, "type": "string",
                  "initial-default": "anonymous", "write-default": "anonymous" }
            ]
        }"#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.schema_id(), &12);
        assert_eq!(
            schema.fields()[0].doc.as_deref(),
            Some("primary identifier")
        );
        assert_eq!(
            schema.fields()[1].initial_default,
            Some(serde_json::Value::String("anonymous".to_string())),
        );
        assert_eq!(
            schema.fields()[1].write_default,
            Some(serde_json::Value::String("anonymous".to_string())),
        );

        let again: Schema = serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
        assert_eq!(again, schema);
    }

    #[test]
    fn test_schema_identifier_field_ids_round_trip() {
        let json = r#"{
            "type": "struct",
            "schema-id": 5,
            "identifier-field-ids": [1, 2],
            "fields": [
                { "id": 1, "name": "first_key",  "required": true,  "type": "int" },
                { "id": 2, "name": "second_key", "required": true,  "type": "int" },
                { "id": 3, "name": "payload",    "required": false, "type": "string" }
            ]
        }"#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.identifier_field_ids(), &Some(vec![1, 2]));

        let serialized = serde_json::to_string(&schema).unwrap();
        assert!(serialized.contains("identifier-field-ids"));
        let again: Schema = serde_json::from_str(&serialized).unwrap();
        assert_eq!(again, schema);
    }

    #[test]
    fn test_schema_nested_struct_list_and_map_round_trip() {
        // Address (nested struct), tags (list of string), attributes (map of string).
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "id",      "required": true,  "type": "long" },
                { "id": 2, "name": "address", "required": false, "type": {
                    "type": "struct",
                    "fields": [
                        { "id": 3, "name": "street", "required": true,  "type": "string" },
                        { "id": 4, "name": "zip",    "required": false, "type": "string" }
                    ]
                } },
                { "id": 5, "name": "tags", "required": false, "type": {
                    "type": "list",
                    "element-id": 6,
                    "element-required": true,
                    "element": "string"
                } },
                { "id": 7, "name": "attributes", "required": false, "type": {
                    "type": "map",
                    "key-id": 8,
                    "key": "string",
                    "value-id": 9,
                    "value-required": false,
                    "value": "string"
                } }
            ]
        }"#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.fields().len(), 4);
        // Cheap shape assertion: types match what the JSON declared.
        assert!(matches!(schema.fields()[1].field_type, Type::Struct(_),));
        assert!(matches!(
            schema.fields()[2].field_type,
            Type::List(ListType { element_id: 6, .. }),
        ));
        assert!(matches!(
            schema.fields()[3].field_type,
            Type::Map(MapType {
                key_id: 8,
                value_id: 9,
                ..
            }),
        ));

        let again: Schema = serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
        assert_eq!(again, schema);
    }

    #[test]
    fn test_schema_v1_without_schema_id_converts_to_v2_with_default() {
        // V1 schemas may omit `schema-id`; converting to V2 (and into the
        // canonical `Schema`) should default it to `DEFAULT_SCHEMA_ID`.
        let v1_json = r#"{
            "type": "struct",
            "fields": [
                { "id": 1, "name": "data", "required": true, "type": "string" }
            ]
        }"#;

        let v1: SchemaV1 = serde_json::from_str(v1_json).unwrap();
        assert!(v1.schema_id.is_none());

        let v2: SchemaV2 = v1.clone().into();
        assert_eq!(v2.schema_id, DEFAULT_SCHEMA_ID);

        let schema: Schema = v1.try_into().unwrap();
        assert_eq!(schema.schema_id(), &DEFAULT_SCHEMA_ID);
        assert_eq!(schema.fields().len(), 1);
    }

    #[test]
    fn test_schema_builder_constructs_expected_struct() {
        let schema = Schema::builder()
            .with_schema_id(42)
            .with_struct_field(primitive_field(1, "k", true, PrimitiveType::Long))
            .with_struct_field(primitive_field(2, "v", false, PrimitiveType::String))
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap();

        assert_eq!(schema.schema_id(), &42);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.identifier_field_ids(), &Some(vec![1]));
        assert_eq!(schema.fields()[0].name, "k");
        assert_eq!(schema.fields()[1].name, "v");
    }

    #[test]
    fn test_schema_project_keeps_listed_fields_and_filters_identifiers() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_struct_field(primitive_field(1, "a", true, PrimitiveType::Long))
            .with_struct_field(primitive_field(2, "b", true, PrimitiveType::Long))
            .with_struct_field(primitive_field(3, "c", true, PrimitiveType::Long))
            .with_identifier_field_ids(vec![1, 3])
            .build()
            .unwrap();

        let projected = schema.project(&[1, 3]);
        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.fields()[0].id, 1);
        assert_eq!(projected.fields()[1].id, 3);
        assert_eq!(projected.identifier_field_ids(), &Some(vec![1, 3]));

        let projected = schema.project(&[2]);
        assert_eq!(projected.fields().len(), 1);
        assert_eq!(projected.fields()[0].id, 2);
        // 2 was never in the identifier set, so projection leaves it empty.
        assert_eq!(projected.identifier_field_ids(), &Some(Vec::<i32>::new()));
    }

    #[test]
    fn test_schema_display_and_fromstr_round_trip() {
        let original = Schema::builder()
            .with_schema_id(99)
            .with_struct_field(primitive_field(1, "flag", false, PrimitiveType::Boolean))
            .with_struct_field(primitive_field(2, "label", true, PrimitiveType::String))
            .build()
            .unwrap();

        let rendered = original.to_string();
        let parsed: Schema = rendered.parse().unwrap();
        assert_eq!(parsed, original);
    }

    // --- TestSchemaParser: rejection cases ---------------------------------
    //
    // The Java SchemaParser refuses malformed schema JSON; these tests
    // pin the corresponding Rust serde behaviour and document gaps where
    // the Rust parser is more lenient than the spec demands.

    #[test]
    #[ignore = "spec gap: Schema parser accepts JSON without a top-level `\"type\": \"struct\"` tag; spec requires it"]
    fn test_schema_parser_rejects_missing_top_level_type() {
        let json = r#"{
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "long" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    #[ignore = "spec gap: Schema parser accepts wrong top-level type tags; spec requires literal `struct`"]
    fn test_schema_parser_rejects_wrong_top_level_type_tag() {
        let json = r#"{
            "type": "table",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "long" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_missing_fields_array() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_field_missing_id() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "name": "a", "required": true, "type": "long" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_field_missing_name() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "required": true, "type": "long" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_field_missing_type() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_unknown_primitive_type() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "biginteger" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    fn test_schema_parser_rejects_malformed_decimal_type() {
        // The decimal parser expects `decimal(precision,scale)`; an empty
        // parameter list or non-numeric parts must be rejected.
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "decimal()" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());

        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "decimal(p,s)" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    #[ignore = "spec gap: Schema parser does not reject duplicate field names within a struct; spec requires rejection"]
    fn test_schema_parser_rejects_duplicate_field_names() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "dup", "required": true, "type": "long" },
                { "id": 2, "name": "dup", "required": true, "type": "string" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    #[test]
    #[ignore = "spec gap: Schema parser does not reject duplicate field ids; spec requires rejection"]
    fn test_schema_parser_rejects_duplicate_field_ids() {
        let json = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "a", "required": true, "type": "long" },
                { "id": 1, "name": "b", "required": true, "type": "string" }
            ]
        }"#;
        assert!(serde_json::from_str::<Schema>(json).is_err());
    }

    // --- TestNameMapping port ----------------------------------------------
    //
    // Java's `org.apache.iceberg.mapping.NameMapping` is the bidirectional
    // bridge between schema field-ids and the human-readable column names
    // used by external file formats (Parquet name-based projection,
    // mapping-driven reads of files written without Iceberg ids).
    //
    // Public API in Java:
    //   - `MappingUtil.create(schema) -> NameMapping`
    //   - `NameMapping.asMappedFields() -> MappedFields`
    //   - `NameMapping(MappedFields)` ctor rejects duplicate top-level names
    //   - `NameMapping.find(int id) -> MappedField | null`
    //   - `NameMapping.find(String... names) -> MappedField | null`
    //     (multi-arg form walks the path; single-arg looks up TOP-LEVEL
    //     names only — nested names like "key" / "element" return null)
    //   - Map types add synthetic "key" / "value" mapped fields under the
    //     map field id.
    //   - List types add a synthetic "element" mapped field under the
    //     list field id.
    //   - Variant types map as a leaf (no nested mapping introspection).
    //
    // Rust has NO `NameMapping` / `MappedField` / `MappingUtil` analog at
    // all (grep across the whole workspace finds zero references). All 12
    // Java scenarios are pinned `#[ignore]` here so an eventual
    // `mapping::NameMapping` module has a ready spec.
    //
    // Expected eventual Rust API (mirroring Java's NameMapping):
    //   - `mapping::MappedField { id: i32, names: Vec<String>, nested: Option<MappedFields> }`
    //   - `mapping::MappedFields(Vec<MappedField>)` (rejects duplicate
    //     top-level names at construction; rejects duplicate top-level ids)
    //   - `mapping::NameMapping::create(&Schema) -> NameMapping`
    //   - `NameMapping::find(id: i32) -> Option<&MappedField>`
    //   - `NameMapping::find_path(path: &[&str]) -> Option<&MappedField>`

    #[test]
    #[ignore = "feature gap: no NameMapping / MappingUtil::create; should produce {1:'id', 2:'data'} for a flat schema"]
    fn test_name_mapping_creates_from_flat_schema() {
        // schema(long id@1, string data@2) -> [MappedField(1, "id"),
        //                                       MappedField(2, "data")].
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; nested struct must yield nested MappedFields under the struct field"]
    fn test_name_mapping_creates_from_nested_struct_schema() {
        // schema(id@1, data@2, location@3 { latitude@4, longitude@5 })
        // -> [MappedField(1, "id"),
        //     MappedField(2, "data"),
        //     MappedField(3, "location", [MappedField(4, "latitude"),
        //                                  MappedField(5, "longitude")])].
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; map type must add synthetic 'key' and 'value' mapped fields under the map field id"]
    fn test_name_mapping_creates_from_map_schema_with_synthetic_key_value() {
        // schema(..., map@3 (string@4 -> double@5))
        // -> MappedField(3, "map", [MappedField(4, "key"),
        //                            MappedField(5, "value")]).
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; map with complex key carries the struct's nested mapping under the synthetic 'key' field"]
    fn test_name_mapping_creates_from_complex_key_map_schema() {
        // schema(..., map@3 (struct@4(x@6, y@7) -> double@5))
        // -> MappedField(3, "map", [MappedField(4, "key",
        //                              [MappedField(6, "x"),
        //                               MappedField(7, "y")]),
        //                            MappedField(5, "value")]).
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; map with complex value carries the struct's nested mapping under the synthetic 'value' field"]
    fn test_name_mapping_creates_from_complex_value_map_schema() {
        // schema(..., map@3 (double@4 -> struct@5(x@6, y@7)))
        // -> MappedField(3, "map", [MappedField(4, "key"),
        //                            MappedField(5, "value",
        //                              [MappedField(6, "x"),
        //                               MappedField(7, "y")])]).
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; list type must add a synthetic 'element' mapped field under the list field id"]
    fn test_name_mapping_creates_from_list_schema_with_synthetic_element() {
        // schema(..., list@3 (string@4))
        // -> MappedField(3, "list", [MappedField(4, "element")]).
    }

    #[test]
    #[ignore = "spec contract: Schema constructor must reject duplicate field ids — currently exists as `test_schema_parser_rejects_duplicate_field_ids` (also #[ignore]); duplicate Java port"]
    fn test_name_mapping_schema_construction_rejects_duplicate_field_id() {
        // Java: `new Schema(required(1, "id", LongType), required(1, "data", StringType))`
        // -> IllegalArgumentException 'Multiple entries with same key: 1=id and 1=data'.
        // Rust counterpart: `Schema::builder().with_struct_field(StructField::new(1,"id",...))
        // .with_struct_field(StructField::new(1,"data",...)).build()` must error.
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; MappedFields constructor must reject duplicate top-level names"]
    fn test_name_mapping_construction_rejects_duplicate_top_level_name() {
        // MappedFields::new(vec![MappedField(1, "x"), MappedField(2, "x")])
        // -> error 'Multiple entries with same key: x=2 and x=1'.
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; duplicate names in DIFFERENT contexts (nested under different parents) are allowed"]
    fn test_name_mapping_allows_duplicate_names_across_separate_contexts() {
        // NameMapping([MappedField(1, "x", [MappedField(3, "x")]),
        //              MappedField(2, "y", [MappedField(4, "x")])])
        // -> OK; each "x" is namespaced by its parent.
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; find(id) walks the full tree; missing id returns None; struct/map/list parents return their nested MappedFields"]
    fn test_name_mapping_find_by_id_descends_through_nested_types() {
        // schema with id@1, data@2, map@3(double key@4 -> struct@5(x@6, y@7)),
        //          list@8(string@9), location@10(latitude@11, longitude@12).
        // find(100) -> None; find(2) -> MappedField(2,"data");
        // find(6) -> MappedField(6,"x"); find(9) -> MappedField(9,"element");
        // find(11) -> MappedField(11,"latitude");
        // find(10) -> MappedField(10,"location", [(11,"latitude"),(12,"longitude")]).
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; find(name) is TOP-LEVEL only — nested synthetic names ('element','x','key','value') return None unless a path is supplied"]
    fn test_name_mapping_find_by_name_is_top_level_unless_path_supplied() {
        // Same fixture.
        // find("element") -> None; find("x") -> None; find("key") -> None;
        // find("value") -> None; find("data") -> MappedField(2,"data");
        // find(["map","value","x"]) -> MappedField(6,"x");
        // find(["list","element"]) -> MappedField(9,"element");
        // find(["location","latitude"]) -> MappedField(11,"latitude");
        // find("location") -> MappedField(10,"location", [...]).
    }

    #[test]
    #[ignore = "feature gap: no NameMapping; V3 VariantType must be mapped as a leaf (no nested MappedFields)"]
    fn test_name_mapping_creates_from_variant_typed_field_as_leaf() {
        // schema(id@1: long, data@2: variant)
        // -> [MappedField(1, "id"), MappedField(2, "data")] — no nested
        // mapping under data because Variant is a leaf type.
    }

    // --- TestHasIds port ---------------------------------------------------
    //
    // Java's `org.apache.iceberg.avro.AvroSchemaUtil.hasIds(avroSchema)`
    // walks an Avro schema and returns true if ANY field carries a
    // `"field-id"` Avro property. Paired with `RemoveIds.removeIds(schema)`
    // it lets readers detect whether an Avro file was written with
    // Iceberg-style ids or needs a NameMapping fallback.
    //
    // Rust handles Avro for manifest reads/writes via the `apache_avro`
    // crate, but does NOT expose an `iceberg::avro::AvroSchemaUtil`
    // facade. There is no `has_ids(avro_schema)` helper anywhere in the
    // workspace (grep finds zero matches).
    //
    // The single Java @Test (`testRemoveIdsHasIds`) exercises three
    // observable scenarios on the same fixture (long id@0, optional
    // map@5 over struct(lat@1, long@2), required list@8 of string@9,
    // required variant@10):
    //
    //   1. After `RemoveIds.removeIds(schema)`, hasIds(result) == false.
    //   2. Adding a field-id prop to the FIRST top-level field flips
    //      hasIds to true.
    //   3. Adding a field-id prop deep inside (the inner struct's "long"
    //      field, reached via union[1] -> valueType -> union[1] ->
    //      field(1)) also flips hasIds to true — so the walk visits the
    //      full tree, not just top-level fields.

    #[test]
    #[ignore = "feature gap: no avro::AvroSchemaUtil::has_ids() / remove_ids(); freshly removed Avro schema should report no ids"]
    fn test_avro_schema_util_has_ids_returns_false_after_remove_ids() {
        // Build the iceberg schema (id@0 long, location@5 map<string,
        // struct(lat@1 float, long@2 float optional)>, types@8 list of
        // required string@9, data@10 variant).
        // remove_ids(schema) -> avro schema with no `field-id` props.
        // has_ids(result) == false.
    }

    #[test]
    #[ignore = "feature gap: no avro::AvroSchemaUtil::has_ids(); adding `field-id` to the FIRST top-level avro field flips has_ids to true"]
    fn test_avro_schema_util_has_ids_returns_true_for_top_level_id() {
        // Take the no-id schema; set fields[0].prop("field-id") = 1.
        // has_ids(result) == true.
    }

    #[test]
    #[ignore = "feature gap: no avro::AvroSchemaUtil::has_ids(); deeply nested field-id (inside map of struct) must also flip has_ids to true"]
    fn test_avro_schema_util_has_ids_walks_into_nested_types() {
        // Take the no-id schema; set the `long` field inside the inner
        // struct (reachable via fields[1] -> union[1] -> valueType ->
        // union[1] -> fields[1]) to have field-id 1.
        // has_ids(result) == true.
    }

    // --- TestSchemaConversions port ----------------------------------------
    //
    // Java's `AvroSchemaUtil.convert(...)` is the bidirectional bridge
    // between Iceberg's type system and Avro's schema model. It's
    // overloaded:
    //
    //   - `AvroSchemaUtil.convert(avroSchema) -> Type` (Avro -> Iceberg).
    //   - `AvroSchemaUtil.convert(type, recordName) -> Schema`
    //   - `AvroSchemaUtil.convert(struct, recordName)` (top-level)
    //   - `AvroSchemaUtil.toIceberg(avroSchema) -> Schema` (Avro -> Iceberg
    //     top-level).
    //
    // Conversion rules pinned by the Java suite:
    //   - Avro primitive types map 1:1 to Iceberg primitives.
    //   - Avro logical types: date->Date, time-micros->Time,
    //     timestamp-micros + adjust-to-utc=true -> Timestamptz,
    //     timestamp-micros + adjust-to-utc=false -> Timestamp,
    //     timestamp-micros (no adjust-to-utc) -> Timestamp (lossy
    //     decode-only; cannot encode).
    //   - Avro uuid (fixed[16] + logical type) -> Iceberg Uuid.
    //   - Avro decimal (fixed) -> Iceberg Decimal.
    //   - Avro `variant` record (metadata: bytes, value: bytes) ->
    //     Iceberg VariantType.
    //   - Iceberg `Map<non-string-key, V>` -> Avro array-of-pair record
    //     (because Avro maps require string keys); Iceberg
    //     `Map<string, V>` -> native Avro map with key/value-id props.
    //   - Iceberg names that aren't valid Avro identifiers are sanitized
    //     (`9x` -> `_9x`, `a.b` -> `a_x2Eb`, ☃ -> `_x2603`, `a#b` ->
    //     `a_x23b`); original name preserved in
    //     `iceberg-field-name` prop.
    //   - Field docs round-trip preserved.
    //   - Iceberg `VariantType` -> Avro record `rN` (id-named) with
    //     metadata + value bytes fields.
    //
    // Rust has NO `avro::AvroSchemaUtil` analog (grep finds zero
    // matches). Manifest read/write uses `apache_avro` directly with
    // hand-rolled type mapping inside `iceberg-rust/src/table/manifest.rs`.
    // All 13 Java @Test scenarios are pinned `#[ignore]` here for an
    // eventual `avro::schema_convert::{to_avro, from_avro}` module.

    #[test]
    #[ignore = "feature gap: no avro::schema_convert; 15 primitives must round-trip Iceberg <-> Avro (Boolean/Int/Long/Float/Double/Date/Time/TimestamptzMicros/TimestampMicros/String/Uuid/Fixed/Binary/Decimal/Variant)"]
    fn test_avro_schema_conversion_primitive_types_per_java() {
        // Java: testPrimitiveTypes (15 primitives, both directions).
        //   bool -> BOOLEAN
        //   int  -> INT
        //   long -> LONG
        //   float -> FLOAT
        //   double -> DOUBLE
        //   Date -> INT + logicalType=date
        //   Time -> LONG + logicalType=time-micros
        //   Timestamptz -> LONG + logicalType=timestamp-micros + adjust-to-utc=true
        //   Timestamp   -> LONG + logicalType=timestamp-micros + adjust-to-utc=false
        //   String -> STRING
        //   Uuid -> FIXED[16] + logicalType=uuid
        //   Fixed(12) -> FIXED[12]
        //   Binary -> BYTES
        //   Decimal(9,4) -> FIXED + logicalType=decimal(9,4)
        //   Variant -> record(metadata: bytes, value: bytes).
    }

    #[test]
    #[ignore = "feature gap: timestamp-micros without an adjust-to-utc Avro prop must decode to Timestamp (withoutZone); reverse direction is lossy and not supported"]
    fn test_avro_schema_conversion_timestamp_without_adjust_to_utc_per_java() {
        // Java: testAvroToIcebergTimestampTypeWithoutAdjustToUTC.
        // Avro: LONG + timestamp-micros logicalType, NO adjust-to-utc prop.
        // -> Iceberg Timestamp.withoutZone() (matches the absent flag).
        // Iceberg's encoded variants always carry adjust-to-utc=true/false,
        // so the reverse direction is intentionally absent from the test.
    }

    #[test]
    #[ignore = "feature gap: top-level struct with all 15 primitives must round-trip Iceberg <-> Avro (named record `primitives`)"]
    fn test_avro_schema_conversion_struct_and_primitive_types_per_java() {
        // Java: testStructAndPrimitiveTypes.
        // Iceberg struct with 15 optional fields named bool/int/long/.../variant
        // -> Avro record('primitives') with field-id props on each field.
    }

    #[test]
    #[ignore = "feature gap: List<Uuid> with element-id prop must round-trip Iceberg <-> Avro"]
    fn test_avro_schema_conversion_list_per_java() {
        // Java: testList. Iceberg ListType.ofRequired(34, Uuid)
        // <-> Avro array with element-id=34 + uuid logical type on items.
    }

    #[test]
    #[ignore = "feature gap: List<Struct(lat, long)> with element-id prop must round-trip via a named inner record `r34`"]
    fn test_avro_schema_conversion_list_of_structs_per_java() {
        // Java: testListOfStructs.
        // Iceberg ListType.ofRequired(34, Struct(lat@35, long@36))
        // <-> Avro array(record('r34', lat: float, long: float)) with
        //     element-id=34, field-ids on inner fields.
    }

    #[test]
    #[ignore = "feature gap: Map<Long, Binary> uses Avro array-of-pair encoding because Avro maps require string keys"]
    fn test_avro_schema_conversion_map_of_long_to_bytes_per_java() {
        // Java: testMapOfLongToBytes.
        // Iceberg MapType.ofRequired(33, 34, Long, Binary)
        // <-> Avro array-of-pair via AvroSchemaUtil.createMap(long, bytes).
    }

    #[test]
    #[ignore = "feature gap: Map<String, Binary> uses native Avro map with key-id + value-id props"]
    fn test_avro_schema_conversion_map_of_string_to_bytes_per_java() {
        // Java: testMapOfStringToBytes.
        // Iceberg MapType.ofRequired(33, 34, String, Binary)
        // <-> Avro map(values: bytes) with key-id=33 + value-id=34 props.
    }

    #[test]
    #[ignore = "feature gap: Map<List<Int>, Struct> requires the non-string-key array-of-pair encoding combined with nested types"]
    fn test_avro_schema_conversion_map_of_list_to_structs_per_java() {
        // Java: testMapOfListToStructs.
        // Iceberg MapType(33, 34, List(35, Int), Struct(a@36, b@37))
        // <-> Avro array-of-pair where key=array(int) (with element-id=35)
        //     and value=record('r34', a: int, b: int|null).
    }

    #[test]
    #[ignore = "feature gap: Map<String, Struct(a, b)> uses native Avro map with the value being a named inner record"]
    fn test_avro_schema_conversion_map_of_string_to_structs_per_java() {
        // Java: testMapOfStringToStructs.
        // Iceberg MapType(33, 34, String, Struct(a@35, b@36))
        // <-> Avro map(values: record('r34', a: int, b: int|null))
        //     with key-id=33 + value-id=34 props.
    }

    #[test]
    #[ignore = "feature gap: arbitrarily nested schema (struct + map<struct, struct> + list<struct> + map<string, string>) must produce a printable Avro schema via AvroSchemaUtil.convert"]
    fn test_avro_schema_conversion_complex_schema_per_java() {
        // Java: testComplexSchema.
        // Schema with id, data, preferences{feature1, feature2},
        // locations: Map<Struct(address, city, state, zip),
        //                Struct(lat, long)>,
        // points: List<Struct(x, y)>,
        // doubles: List<Double>,
        // properties: Map<String, String>.
        // -> AvroSchemaUtil.convert(schema, "newTableName") produces
        //    a printable Avro schema via SchemaFormatter("json/pretty").
    }

    #[test]
    #[ignore = "feature gap: Iceberg field names that aren't valid Avro identifiers are sanitized; original preserved in `iceberg-field-name` prop"]
    fn test_avro_schema_conversion_special_chars_per_java() {
        // Java: testSpecialChars.
        // Iceberg fields ["9x", "x_", "a.b", "☃", "a#b"] sanitize to
        // ["_9x", "x_", "a_x2Eb", "_x2603", "a_x23b"].
        // For each sanitized field, the original Iceberg name is preserved
        // in the iceberg-field-name Avro prop (except "x_" which is valid
        // Avro and thus has no prop set).
    }

    #[test]
    #[ignore = "feature gap: per-field doc strings must survive Iceberg -> Avro -> Iceberg round-trip"]
    fn test_avro_schema_conversion_field_docs_preserved_per_java() {
        // Java: testFieldDocsArePreserved.
        // Iceberg schema with fieldDocs [null, "iceberg originating field doc"]
        // round-trips through AvroSchemaUtil.convert + AvroSchemaUtil.toIceberg
        // preserving the doc list unchanged.
    }

    #[test]
    #[ignore = "feature gap: Variant fields encode as Avro record `rN` (named after the field id) with metadata: bytes + value: bytes children"]
    fn test_avro_schema_conversion_variant_record_layout_per_java() {
        // Java: testVariantConversion.
        // Schema with two Variant fields variantCol1@1, variantCol2@2.
        // For each id ∈ {1, 2}:
        //   variantSchema.name == "r" + id;
        //   variantSchema.type == RECORD;
        //   variantSchema.fields.size == 2;
        //   variantSchema.field("metadata").schema.type == BYTES;
        //   variantSchema.field("value").schema.type == BYTES.
    }
}
