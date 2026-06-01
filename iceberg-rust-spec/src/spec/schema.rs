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

    // -----------------------------------------------------------------------
    // Placeholders for a future `schema_update` module.
    //
    // iceberg-rust-spec has no `SchemaUpdate` builder; schema evolution lives
    // only as `TableUpdate::AddSchema` / `TableUpdate::SetCurrentSchema` at
    // catalog-commit time. The fluent ops below (add/delete/rename/update/move
    // columns, identifier-field management, case-sensitivity, nested-type
    // descent) all need a `schema_update::SchemaUpdate::new(&Schema, last_column_id)`
    // surface to land. Each test pins one observable scenario the eventual
    // implementation must satisfy.
    // -----------------------------------------------------------------------

    // --- no-op + update column type/doc/default ---

    #[test]
    #[ignore = "no schema_update module"]
    fn test_schema_update_no_changes_returns_input_schema() {
        // SchemaUpdate(schema).apply() with no operations returns a schema equal to input.
        unimplemented!("schema_update");
    }

    #[test]
    #[ignore = "no schema_update module"]
    fn test_delete_columns_drops_named_fields() {
        // delete_column("id"), delete_column("data") drop two top-level fields.
        unimplemented!("schema_update::delete_column");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false)"]
    fn test_delete_columns_case_insensitive_matches_any_case() {
        // case_sensitive(false).delete_column("ID") drops "id".
        unimplemented!("schema_update case-insensitive");
    }

    #[test]
    #[ignore = "no schema_update::update_column"]
    fn test_update_column_widens_primitive_types_for_allowed_promotions() {
        // Int->Long, Float->Double, Decimal precision-widening all succeed.
        unimplemented!("schema_update::update_column");
    }

    #[test]
    #[ignore = "no schema_update::update_column"]
    fn test_update_column_type_preserves_doc_default_required_flag() {
        // Field with doc + initial/write defaults + required flag keeps all of
        // them after update_column changes its type.
        unimplemented!("schema_update::update_column metadata preservation");
    }

    #[test]
    #[ignore = "no schema_update::update_column_doc"]
    fn test_update_column_doc_preserves_type_default_required_flag() {
        // update_column_doc("a","new doc") preserves type, defaults, required flag.
        unimplemented!("schema_update::update_column_doc");
    }

    #[test]
    #[ignore = "no schema_update::update_column_default"]
    fn test_update_column_default_preserves_type_doc_required_flag() {
        // update_column_default("a", Literal::of(99)) preserves type, doc, required.
        unimplemented!("schema_update::update_column_default");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false)"]
    fn test_update_column_type_case_insensitive_match() {
        // case_sensitive(false).update_column("ID", Long) resolves to lower-case "id".
        unimplemented!("schema_update case-insensitive");
    }

    #[test]
    #[ignore = "no schema_update::update_column"]
    fn test_update_column_rejects_disallowed_type_promotion() {
        // Long->Int, Double->Float, narrowing decimals all raise IllegalArgument.
        unimplemented!("schema_update::update_column rejection");
    }

    // --- rename column ---

    #[test]
    #[ignore = "no schema_update::rename_column"]
    fn test_rename_column_changes_name_only() {
        // rename_column("id","row_id") preserves id, type, doc, defaults.
        unimplemented!("schema_update::rename_column");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false)"]
    fn test_rename_column_case_insensitive_match() {
        // case_sensitive(false).rename_column("ID","row_id") resolves to lower-case.
        unimplemented!("schema_update case-insensitive rename");
    }

    // --- add column (top-level + nested) ---

    #[test]
    #[ignore = "no schema_update::add_column"]
    fn test_add_column_appends_to_top_level() {
        // add_column(None,"score",Float,None) appends with the next fresh id.
        unimplemented!("schema_update::add_column");
    }

    #[test]
    #[ignore = "no schema_update::add_column with initial_default"]
    fn test_add_column_with_initial_default_value() {
        // add_column with initial_default=Literal::of(7) sets initial-default on the new field.
        unimplemented!("schema_update::add_column initial_default");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_add_column_then_update_column_default() {
        // add_column("score",Float) then update_column_default("score",Literal::of(0.5))
        // applies as a single commit, with the default landing on the new field.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::add_column with parent"]
    fn test_add_column_inside_nested_struct() {
        // add_column(Some("location"),"altitude",Float,None) lands inside the location struct.
        unimplemented!("schema_update::add_column parent");
    }

    #[test]
    #[ignore = "no schema_update::add_column inside map-of-struct value"]
    fn test_add_field_inside_map_value_struct() {
        // add_column(Some("mapField.value"),"info",String,None) lands inside the map value struct.
        unimplemented!("schema_update::add_column map value");
    }

    #[test]
    #[ignore = "no schema_update::add_column inside list-of-struct element"]
    fn test_add_field_inside_list_element_struct() {
        // add_column(Some("list.element"),"weight",Int,None) lands inside the list element struct.
        unimplemented!("schema_update::add_column list element");
    }

    #[test]
    #[ignore = "no schema_update::add_required_column without default rejection"]
    fn test_add_required_column_without_default_rejected() {
        // add_required_column with no default raises IllegalArgument
        // because existing rows would have null for the new required column.
        unimplemented!("schema_update::add_required_column rejection");
    }

    #[test]
    #[ignore = "no schema_update::add_required_column"]
    fn test_add_required_column_with_default_accepted() {
        // add_required_column(name,type,default=Literal::of(0)) succeeds; existing rows
        // read as the default for this column.
        unimplemented!("schema_update::add_required_column");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_add_required_column_via_update_column_default() {
        // add_column(optional) then update_column_default(...) then require_column(...) succeeds.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false) for add_required_column"]
    fn test_add_required_column_case_insensitive_match() {
        // case_sensitive(false).add_required_column resolves any conflict by case-insensitive lookup.
        unimplemented!("schema_update case-insensitive");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false) for multi-add"]
    fn test_add_multiple_required_columns_case_insensitive() {
        // case_sensitive(false) + multiple add_required_column ops in one apply.
        unimplemented!("schema_update case-insensitive");
    }

    // --- make optional / require ---

    #[test]
    #[ignore = "no schema_update::make_column_optional"]
    fn test_make_column_optional_flips_required_flag() {
        // make_column_optional("id") flips required=true to required=false.
        unimplemented!("schema_update::make_column_optional");
    }

    #[test]
    #[ignore = "no schema_update::require_column"]
    fn test_require_column_flips_optional_to_required() {
        // require_column("data") flips required=false to required=true on a column with default.
        unimplemented!("schema_update::require_column");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_add_then_require_column_with_default() {
        // add_column(name,type,initial_default) then require_column(name) succeeds.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_add_then_require_column_via_update_column_default() {
        // add_column(optional) then update_column_default then require_column succeeds.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false) for require_column"]
    fn test_require_column_case_insensitive_match() {
        // case_sensitive(false).require_column("DATA") resolves to lower-case "data".
        unimplemented!("schema_update case-insensitive");
    }

    // --- mixed change set ---

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_mixed_add_delete_rename_update_in_one_apply() {
        // A single apply() with delete + rename + update + add operations produces a
        // schema reflecting all four edits at once.
        unimplemented!("schema_update chained ops");
    }

    // --- conflict detection ---

    #[test]
    #[ignore = "no schema_update::add_column ambiguity rejection"]
    fn test_add_column_rejects_ambiguous_dotted_path() {
        // Schema where "a.b" exists as both a flat field name and a nested path
        // → add_column("a.b","c",Int) rejects with "ambiguous dotted path".
        unimplemented!("schema_update::add_column ambiguity");
    }

    #[test]
    #[ignore = "no schema_update::add_column duplicate rejection"]
    fn test_add_column_rejects_when_name_already_exists() {
        // add_column("id",Long) rejects because "id" already exists at the target parent.
        unimplemented!("schema_update::add_column duplicate");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_delete_then_add_same_name_succeeds_with_new_id() {
        // delete_column("id") + add_column("id",Long) yields a new id for the readded column.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_delete_then_add_inside_nested_struct() {
        // Inside a nested struct: delete a field then re-add same name with new id.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::delete_column missing rejection"]
    fn test_delete_column_rejects_missing_name() {
        // delete_column("absent") raises IllegalArgument.
        unimplemented!("schema_update::delete_column missing");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_add_then_delete_same_name_rejected() {
        // add_column("new",Int) then delete_column("new") rejects because the column was just added.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::rename_column missing rejection"]
    fn test_rename_column_rejects_missing_source_name() {
        // rename_column("absent","new") raises IllegalArgument.
        unimplemented!("schema_update::rename_column missing");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_rename_then_delete_target_name_rejected() {
        // rename_column("a","b") then delete_column("b") rejects because "b" was just renamed in.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_delete_then_rename_to_deleted_name_rejected() {
        // delete_column("a") then rename_column("b","a") rejects because "a" was just deleted.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update::update_column missing rejection"]
    fn test_update_column_rejects_missing_name() {
        // update_column("absent",Long) raises IllegalArgument.
        unimplemented!("schema_update::update_column missing");
    }

    #[test]
    #[ignore = "no schema_update::update_column_doc missing rejection"]
    fn test_update_column_doc_rejects_missing_name() {
        // update_column_doc("absent","doc") raises IllegalArgument.
        unimplemented!("schema_update::update_column_doc missing");
    }

    #[test]
    #[ignore = "no schema_update::update_column_default missing rejection"]
    fn test_update_column_default_rejects_missing_name() {
        // update_column_default("absent",Literal::of(0)) raises IllegalArgument.
        unimplemented!("schema_update::update_column_default missing");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_update_then_delete_same_name_rejected() {
        // update_column then delete_column on the same name rejects.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_delete_then_update_same_name_rejected() {
        // delete_column then update_column on the same name rejects.
        unimplemented!("schema_update chained ops");
    }

    // --- map field ops ---

    #[test]
    #[ignore = "no schema_update map-key delete rejection"]
    fn test_delete_map_key_rejected() {
        // delete_column("mapField.key") raises IllegalArgument; map keys are not deletable.
        unimplemented!("schema_update map key delete");
    }

    #[test]
    #[ignore = "no schema_update map-value delete rejection"]
    fn test_delete_map_value_rejected() {
        // delete_column("mapField.value") raises IllegalArgument; map values are not deletable.
        unimplemented!("schema_update map value delete");
    }

    #[test]
    #[ignore = "no schema_update::add_column inside map-key struct"]
    fn test_add_field_to_map_key_struct() {
        // add_column(Some("mapField.key"),"x",Int,None) lands inside the map-key struct.
        unimplemented!("schema_update map key add");
    }

    #[test]
    #[ignore = "no schema_update map-key type alter rejection"]
    fn test_alter_map_key_struct_type_rejected() {
        // update_column("mapField.key",NewType) raises IllegalArgument; key type cannot change.
        unimplemented!("schema_update map key alter");
    }

    #[test]
    #[ignore = "no schema_update map-key primitive promotion"]
    fn test_update_map_key_primitive_promotion() {
        // For primitive map keys, update_column promotes via the same allowed-promotion rules.
        unimplemented!("schema_update map key promote");
    }

    // --- update on freshly added / deleted columns ---

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_update_type_on_freshly_added_column() {
        // add_column(name,Int) then update_column(name,Long) succeeds within the same apply.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_update_doc_on_freshly_added_column() {
        // add_column(name,Int) then update_column_doc(name,"new") sets the doc on the new field.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained ops"]
    fn test_update_doc_on_deleted_column_rejected() {
        // delete_column(name) then update_column_doc(name,"doc") rejects.
        unimplemented!("schema_update chained ops");
    }

    // --- move ops (top-level + nested) ---

    #[test]
    #[ignore = "no schema_update::move_xxx ops"]
    fn test_multiple_consecutive_moves_apply_in_sequence() {
        // Three consecutive move ops (first/before/after) compose into one final ordering.
        unimplemented!("schema_update move");
    }

    #[test]
    #[ignore = "no schema_update::move_first"]
    fn test_move_first_places_top_level_column_at_position_zero() {
        // move_first("data") puts "data" at index 0 of the top-level fields.
        unimplemented!("schema_update::move_first");
    }

    #[test]
    #[ignore = "no schema_update::move_before"]
    fn test_move_before_first_places_column_at_position_zero() {
        // move_before("data","id") with id at index 0 puts "data" at index 0 (and id at 1).
        unimplemented!("schema_update::move_before");
    }

    #[test]
    #[ignore = "no schema_update::move_after"]
    fn test_move_after_last_places_column_at_end() {
        // move_after("id","data") with data the last field puts id at the new last position.
        unimplemented!("schema_update::move_after");
    }

    #[test]
    #[ignore = "no schema_update::move_after"]
    fn test_move_after_sibling_places_column_after_named_sibling() {
        // move_after("a","c") between two other fields places a directly after c.
        unimplemented!("schema_update::move_after");
    }

    #[test]
    #[ignore = "no schema_update::move_before"]
    fn test_move_before_sibling_places_column_before_named_sibling() {
        // move_before("a","c") places a directly before c.
        unimplemented!("schema_update::move_before");
    }

    #[test]
    #[ignore = "no schema_update::move_first nested"]
    fn test_move_nested_field_first_within_parent_struct() {
        // move_first("location.lat") puts lat at index 0 within the location struct.
        unimplemented!("schema_update::move_first nested");
    }

    #[test]
    #[ignore = "no schema_update::move_before nested"]
    fn test_move_nested_field_before_first_within_parent_struct() {
        // move_before("location.lat","location.long") with long at index 0 puts lat at 0.
        unimplemented!("schema_update::move_before nested");
    }

    #[test]
    #[ignore = "no schema_update::move_after nested"]
    fn test_move_nested_field_after_last_within_parent_struct() {
        // move_after on the last sibling of a nested struct is a no-op for the moved field.
        unimplemented!("schema_update::move_after nested");
    }

    #[test]
    #[ignore = "no schema_update::move_after nested"]
    fn test_move_nested_field_after_named_sibling() {
        // move_after("location.long","location.lat") within a 3-field nested struct.
        unimplemented!("schema_update::move_after nested");
    }

    #[test]
    #[ignore = "no schema_update::move_before nested"]
    fn test_move_nested_field_before_named_sibling() {
        // move_before within a 3-field nested struct.
        unimplemented!("schema_update::move_before nested");
    }

    #[test]
    #[ignore = "no schema_update::move within list element struct"]
    fn test_move_field_within_list_element_struct() {
        // move within list.element.struct preserves the list shape and reorders inside.
        unimplemented!("schema_update::move list element");
    }

    #[test]
    #[ignore = "no schema_update::move within map value struct"]
    fn test_move_field_within_map_value_struct() {
        // move within map.value.struct preserves the map shape and reorders inside.
        unimplemented!("schema_update::move map value");
    }

    #[test]
    #[ignore = "no schema_update chained add+move ops"]
    fn test_move_freshly_added_top_level_column() {
        // add_column("new") then move_first("new") puts "new" at index 0.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained add+move ops"]
    fn test_move_added_top_level_column_after_another_added_column() {
        // add x, add y, move_after("x","y") yields ...,y,x,... ordering.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained add+move ops"]
    fn test_move_freshly_added_nested_struct_field() {
        // add_column(Some("location"),"altitude",...) then move_first("location.altitude").
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update chained add+move ops"]
    fn test_move_added_nested_struct_field_before_another_added_field() {
        // Two adds inside a struct, then move_before on the new ones.
        unimplemented!("schema_update chained ops");
    }

    #[test]
    #[ignore = "no schema_update move self-reference rejection"]
    fn test_move_field_to_self_position_rejected() {
        // move_before("a","a") and move_after("a","a") raise IllegalArgument.
        unimplemented!("schema_update::move self-ref");
    }

    #[test]
    #[ignore = "no schema_update move missing rejection"]
    fn test_move_rejects_missing_source_column() {
        // move_first("absent") raises IllegalArgument.
        unimplemented!("schema_update::move missing");
    }

    #[test]
    #[ignore = "no schema_update move-before-add rejection"]
    fn test_move_before_added_column_in_same_apply_rejected() {
        // move_before("existing","new") where "new" is being added in this apply rejects.
        unimplemented!("schema_update::move-before-add");
    }

    #[test]
    #[ignore = "no schema_update move missing reference rejection"]
    fn test_move_rejects_missing_reference_column() {
        // move_after("a","absent") raises IllegalArgument.
        unimplemented!("schema_update::move missing reference");
    }

    #[test]
    #[ignore = "no schema_update map-key move rejection"]
    fn test_move_primitive_map_key_rejected() {
        // move on a primitive map-key field rejects (no sibling structure).
        unimplemented!("schema_update::move map key");
    }

    #[test]
    #[ignore = "no schema_update map-value move rejection"]
    fn test_move_primitive_map_value_rejected() {
        // move on a primitive map-value field rejects.
        unimplemented!("schema_update::move map value");
    }

    #[test]
    #[ignore = "no schema_update list-element move rejection"]
    fn test_move_primitive_list_element_rejected() {
        // move on a primitive list-element field rejects.
        unimplemented!("schema_update::move list element");
    }

    #[test]
    #[ignore = "no schema_update cross-struct move rejection"]
    fn test_move_top_level_field_between_different_structs_rejected() {
        // move_before("a","other_struct.b") rejects because the target lives in a different parent.
        unimplemented!("schema_update::move cross-struct");
    }

    #[test]
    #[ignore = "no schema_update cross-struct move rejection"]
    fn test_move_nested_field_to_sibling_struct_rejected() {
        // move within a nested struct rejects when the target sibling is in a different struct.
        unimplemented!("schema_update::move cross-struct nested");
    }

    // --- identifier field management ---

    #[test]
    #[ignore = "no schema_update::set_identifier_fields"]
    fn test_set_identifier_fields_accepts_existing_field_names() {
        // set_identifier_fields(["id"]) succeeds for an existing required column.
        unimplemented!("schema_update::set_identifier_fields");
    }

    #[test]
    #[ignore = "no schema_update chained ops with identifier fields"]
    fn test_set_identifier_fields_on_freshly_added_columns() {
        // add_column(name) then set_identifier_fields([name]) succeeds.
        unimplemented!("schema_update chained identifier");
    }

    #[test]
    #[ignore = "no schema_update::set_identifier_fields nested"]
    fn test_set_identifier_fields_inside_nested_struct() {
        // set_identifier_fields(["location.lat"]) targets a nested field id.
        unimplemented!("schema_update::set_identifier_fields nested");
    }

    #[test]
    #[ignore = "no schema_update::set_identifier_fields dotted-path"]
    fn test_set_identifier_fields_via_dotted_path() {
        // Dotted-path argument resolves to the nested field id.
        unimplemented!("schema_update::set_identifier_fields dotted");
    }

    #[test]
    #[ignore = "no schema_update::remove_identifier_field"]
    fn test_remove_identifier_field_clears_from_set() {
        // remove_identifier_field("id") drops id from the identifier set.
        unimplemented!("schema_update::remove_identifier_field");
    }

    #[test]
    #[ignore = "no schema_update::set_identifier_fields invalid-target rejection"]
    fn test_set_identifier_fields_rejects_invalid_combinations() {
        // Optional column, float/double/null/missing column, doubly-nested-in-struct paths
        // all raise IllegalArgument for set_identifier_fields.
        unimplemented!("schema_update::set_identifier_fields rejection");
    }

    #[test]
    #[ignore = "no schema_update chained delete+identifier ops"]
    fn test_delete_identifier_field_column_clears_id_from_set() {
        // delete_column("id") drops id from identifier_field_ids automatically.
        unimplemented!("schema_update chained delete identifier");
    }

    #[test]
    #[ignore = "no schema_update delete-last-identifier rejection"]
    fn test_delete_only_remaining_identifier_field_column_rejected() {
        // delete_column on the only column in identifier_field_ids without compensating ops rejects.
        unimplemented!("schema_update delete last identifier");
    }

    #[test]
    #[ignore = "no schema_update delete-containing-identifier rejection"]
    fn test_delete_struct_that_contains_identifier_field_rejected() {
        // delete_column("location") when "location.lat" is identifier rejects.
        unimplemented!("schema_update delete containing identifier");
    }

    #[test]
    #[ignore = "no schema_update::rename_column propagates identifier id"]
    fn test_rename_identifier_field_preserves_id_in_set() {
        // rename_column on an identifier field keeps the id in identifier_field_ids.
        unimplemented!("schema_update rename identifier");
    }

    #[test]
    #[ignore = "no schema_update::move on identifier field"]
    fn test_move_identifier_field_preserves_id_in_set() {
        // move on an identifier field keeps the id in identifier_field_ids.
        unimplemented!("schema_update move identifier");
    }

    #[test]
    #[ignore = "no schema_update::case_sensitive(false) for move on identifier field"]
    fn test_move_identifier_field_case_insensitive_match() {
        // case_sensitive(false).move_first("ID") works on the identifier field "id".
        unimplemented!("schema_update case-insensitive move identifier");
    }

    // --- move on deleted columns ---

    #[test]
    #[ignore = "no schema_update::move on deleted column"]
    fn test_move_after_deleted_top_level_column_rejected() {
        // delete_column("a") then move_after("b","a") rejects because "a" was just deleted.
        unimplemented!("schema_update move after deleted");
    }

    #[test]
    #[ignore = "no schema_update::move on deleted column"]
    fn test_move_before_deleted_top_level_column_rejected() {
        // delete_column("a") then move_before("b","a") rejects.
        unimplemented!("schema_update move before deleted");
    }

    #[test]
    #[ignore = "no schema_update::move on deleted column"]
    fn test_move_first_of_deleted_top_level_column_rejected() {
        // delete_column("a") then move_first("a") rejects.
        unimplemented!("schema_update move first deleted");
    }

    #[test]
    #[ignore = "no schema_update::move on deleted nested column"]
    fn test_move_after_deleted_nested_field_rejected() {
        // delete nested then move_after referencing it rejects.
        unimplemented!("schema_update move after deleted nested");
    }

    #[test]
    #[ignore = "no schema_update::move on deleted nested column"]
    fn test_move_before_deleted_nested_field_rejected() {
        // delete nested then move_before referencing it rejects.
        unimplemented!("schema_update move before deleted nested");
    }

    #[test]
    #[ignore = "no schema_update::move on deleted nested column"]
    fn test_move_first_of_deleted_nested_field_rejected() {
        // delete nested then move_first on it rejects.
        unimplemented!("schema_update move first deleted nested");
    }

    // --- V3 Unknown type handling ---

    #[test]
    #[ignore = "no schema_update::add_column for Unknown"]
    fn test_add_column_with_unknown_type_succeeds() {
        // add_column(name,Unknown,None) succeeds; existing rows null-fill.
        unimplemented!("schema_update::add_column unknown");
    }

    #[test]
    #[ignore = "no schema_update Unknown non-null default rejection"]
    fn test_add_unknown_column_with_non_null_default_rejected() {
        // add_column(name,Unknown,Literal::of(...)) rejects because Unknown has no defaultable values.
        unimplemented!("schema_update unknown non-null default");
    }

    #[test]
    #[ignore = "no schema_update required Unknown rejection"]
    fn test_add_required_unknown_column_rejected() {
        // add_required_column(name,Unknown,...) rejects because Unknown values cannot satisfy required.
        unimplemented!("schema_update required unknown");
    }

    // --- case-insensitive add + move combinations ---

    #[test]
    #[ignore = "no schema_update case-insensitive add+move"]
    fn test_case_insensitive_add_top_level_and_move() {
        // case_sensitive(false).add_column("X",Int) then move_first("X") with target lookup of "x".
        unimplemented!("schema_update case-insensitive add+move");
    }

    #[test]
    #[ignore = "no schema_update case-insensitive nested add+move"]
    fn test_case_insensitive_add_nested_and_move() {
        // case_sensitive(false) for nested add + nested move with mixed-case names.
        unimplemented!("schema_update case-insensitive nested add+move");
    }

    #[test]
    #[ignore = "no schema_update case-insensitive move after newly added"]
    fn test_case_insensitive_move_after_newly_added_field() {
        // case_sensitive(false).add_column then move_after referencing the added column by alt case.
        unimplemented!("schema_update case-insensitive move-after-add");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `schema_update::union_by_name` reducer.
    //
    // No Rust analog exists. Eventual signature:
    //   schema_update::union_by_name(&Schema, last_column_id: i32, &Schema) -> Result<Schema, Error>
    // Merges incoming schema by field NAME (not id): existing names keep their
    // ids; new names get fresh ids; primitive promotion is honoured (widening
    // only); structural changes (list element type, map key/value type, list
    // to primitive) are rejected.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_top_level_primitive() {
        // Existing {id (1,int)} unioned with {id, score (float)} yields
        // {id (1), score (fresh id)}.
        unimplemented!("schema_update::union_by_name");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_propagates_field_default() {
        // Incoming field carrying initial-default/write-default produces a new field with
        // those defaults attached.
        unimplemented!("schema_update::union_by_name defaults");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_top_level_list() {
        // Union introduces a new top-level list-of-primitive with a fresh element id.
        unimplemented!("schema_update::union_by_name top-level list");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_top_level_map() {
        // Union introduces a new top-level map<string,int> with fresh key/value ids.
        unimplemented!("schema_update::union_by_name top-level map");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_top_level_struct() {
        // Union introduces a new top-level struct with fresh field ids inside.
        unimplemented!("schema_update::union_by_name top-level struct");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_field_inside_nested_struct() {
        // Union adds a single primitive inside an already-existing nested struct.
        unimplemented!("schema_update::union_by_name nested primitive");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_deeply_nested_primitives() {
        // 9-level-deep struct chain in the union schema: every newly added node gets a fresh id;
        // every existing node keeps its id.
        unimplemented!("schema_update::union_by_name deeply nested primitives");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_deeply_nested_lists() {
        // 9-level-deep list-of-list-of-... chain merges with id preservation.
        unimplemented!("schema_update::union_by_name deeply nested lists");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_struct_inside_struct() {
        // Union introduces a struct inside an existing struct.
        unimplemented!("schema_update::union_by_name struct in struct");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name"]
    fn test_union_by_name_adds_deeply_nested_maps() {
        // 6-level-deep map<string, map<...>> chain merges with id preservation.
        unimplemented!("schema_update::union_by_name deeply nested maps");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name list-element type-change rejection"]
    fn test_union_by_name_rejects_list_element_type_change() {
        // Existing list<int> + incoming list<long> → reject (element type changed).
        unimplemented!("schema_update::union_by_name list element type rejection");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name map-value type-change rejection"]
    fn test_union_by_name_rejects_map_value_type_change() {
        // Existing map<string,int> + incoming map<string,long> → reject (value type changed).
        unimplemented!("schema_update::union_by_name map value type rejection");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name map-key type-change rejection"]
    fn test_union_by_name_rejects_map_key_type_change() {
        // Existing map<string,int> + incoming map<long,int> → reject (key type changed).
        unimplemented!("schema_update::union_by_name map key type rejection");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name doc propagation"]
    fn test_union_by_name_propagates_doc_changes() {
        // Incoming field with a doc replaces the existing field's doc (or sets it if none).
        unimplemented!("schema_update::union_by_name doc");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name default propagation"]
    fn test_union_by_name_propagates_write_default_preserves_initial() {
        // Incoming write-default overwrites; initial-default is never overwritten.
        unimplemented!("schema_update::union_by_name defaults");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name int->long promotion"]
    fn test_union_by_name_promotes_int_to_long() {
        // Existing Int field + incoming Long with same name → result is Long.
        unimplemented!("schema_update::union_by_name int promote");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name float->double promotion"]
    fn test_union_by_name_promotes_float_to_double() {
        // Float + Double → Double.
        unimplemented!("schema_update::union_by_name float promote");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name double->float narrowing ignored"]
    fn test_union_by_name_ignores_double_to_float_narrowing() {
        // Existing Double + incoming Float keeps Double (narrowing ignored).
        unimplemented!("schema_update::union_by_name double narrow");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name long->int narrowing ignored"]
    fn test_union_by_name_ignores_long_to_int_narrowing() {
        // Existing Long + incoming Int keeps Long (narrowing ignored).
        unimplemented!("schema_update::union_by_name long narrow");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name decimal narrowing ignored"]
    fn test_union_by_name_ignores_decimal_narrowing() {
        // Existing Decimal(10,2) + incoming Decimal(7,2) keeps the wider precision.
        unimplemented!("schema_update::union_by_name decimal narrow");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name decimal widening"]
    fn test_union_by_name_promotes_decimal_to_wider_precision_same_scale() {
        // Existing Decimal(7,2) + incoming Decimal(10,2) yields Decimal(10,2).
        unimplemented!("schema_update::union_by_name decimal widen");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name nested struct add"]
    fn test_union_by_name_adds_primitive_to_existing_nested_struct() {
        // Existing struct{a,b} + incoming struct{a,b,c} → struct keeps a,b ids and adds fresh-id c.
        unimplemented!("schema_update::union_by_name nested struct add");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name list-to-primitive rejection"]
    fn test_union_by_name_rejects_list_to_primitive_change() {
        // Existing list field + incoming primitive of same name → reject.
        unimplemented!("schema_update::union_by_name list to primitive");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name mirrored schemas"]
    fn test_union_by_name_mirrored_schemas_preserve_ids() {
        // Union of a schema with itself returns the same schema (ids preserved, no new fields).
        unimplemented!("schema_update::union_by_name mirrored");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name append sibling struct"]
    fn test_union_by_name_appends_sibling_struct_at_depth() {
        // Adds a sibling struct several levels deep with fresh ids inside.
        unimplemented!("schema_update::union_by_name sibling struct");
    }

    #[test]
    #[ignore = "no schema_update::union_by_name append sibling list"]
    fn test_union_by_name_appends_sibling_list_at_depth() {
        // Adds a sibling list several levels deep with fresh element id.
        unimplemented!("schema_update::union_by_name sibling list");
    }

    // -----------------------------------------------------------------------
    // Placeholders for joint schema + name-mapping evolution.
    //
    // No Rust analog. Eventual surface: a `NameMapping` value that can be
    // refreshed alongside schema_update commits, plus reactive updates to
    // table-property-driven metrics/bloom/column-stats configs when a column
    // is renamed/deleted.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no NameMapping; no schema_update::add_column join"]
    fn test_add_column_refreshes_name_mapping_with_new_field() {
        // After SchemaUpdate.add_column commits, NameMapping::create over the new schema
        // includes the new column with its newly assigned id.
        unimplemented!("NameMapping + schema_update add");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update join"]
    fn test_add_struct_column_refreshes_name_mapping_into_nested_field_tree() {
        // Adding a nested struct column reflects in NameMapping with the inner field tree.
        unimplemented!("NameMapping + schema_update nested add");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update::rename_column join"]
    fn test_rename_column_keeps_id_and_preserves_alias_in_name_mapping() {
        // Renaming a column keeps its id; NameMapping carries both the new name and
        // the old name as an alias of the same id.
        unimplemented!("NameMapping + schema_update rename");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update::delete_column join"]
    fn test_delete_column_drops_field_from_name_mapping() {
        // delete_column drops the entry from NameMapping after commit.
        unimplemented!("NameMapping + schema_update delete");
    }

    #[test]
    #[ignore = "no write.metadata.metrics.column.<name> table property handling"]
    fn test_schema_op_updates_metrics_table_property_on_rename_and_delete() {
        // write.metadata.metrics.column.<old_name> is renamed/removed when the column is
        // renamed/deleted.
        unimplemented!("metrics property propagation");
    }

    #[test]
    #[ignore = "no write.parquet.bloom-filter-enabled.column.<name> table property handling"]
    fn test_schema_op_updates_parquet_bloom_table_property_on_rename_and_delete() {
        // write.parquet.bloom-filter-enabled.column.<old_name> is renamed/removed.
        unimplemented!("bloom property propagation");
    }

    #[test]
    #[ignore = "no write.parquet.column-stats-enabled.column.<name> table property handling"]
    fn test_schema_op_updates_parquet_column_stats_table_property_on_rename_and_delete() {
        // write.parquet.column-stats-enabled.column.<old_name> is renamed/removed.
        unimplemented!("column-stats property propagation");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update chained delete+add"]
    fn test_delete_then_add_same_name_yields_fresh_id_and_resets_name_mapping_entry() {
        // delete + re-add same name commits as two ops; NameMapping uses the new id with
        // the old name as alias if previously aliased.
        unimplemented!("NameMapping + schema_update delete+add");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update chained delete+rename"]
    fn test_delete_then_rename_to_freed_name_succeeds_with_mapping_reassign() {
        // delete column "A", rename column "B" to "A" succeeds; NameMapping reflects the new
        // id-to-name binding.
        unimplemented!("NameMapping + schema_update delete+rename");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update chained rename+add"]
    fn test_rename_then_add_with_freed_name_succeeds_with_mapping_reassign() {
        // rename "A"->"X" frees the old name "A"; then add a fresh "A" → fresh id, alias chain reset.
        unimplemented!("NameMapping + schema_update rename+add");
    }

    #[test]
    #[ignore = "no NameMapping; no schema_update chained two-renames"]
    fn test_two_renames_swap_field_names_with_consistent_mapping_aliases() {
        // rename A->B, rename C->A succeeds; NameMapping carries the alias chains for both.
        unimplemented!("NameMapping + schema_update two-renames");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `avro::schema_util::prune_columns` helper.
    //
    // Eventual signature:
    //   prune_columns(&avro::Schema, &HashSet<i32>) -> avro::Schema
    // Returns a pruned Avro schema matching the Iceberg-schema projection: for
    // map / list / struct fields, selecting any of the container's ids (incl.
    // synthetic key/element/value ids) projects the whole container; selecting
    // a child id of a struct projects just that child wrapped in its parent.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_returns_only_selected_top_level_primitive() {
        // Schema with id (Long), properties (Map), location (Struct), tags (List), payload (Variant).
        // prune({0}) → schema with just id.
        unimplemented!("avro::schema_util::prune_columns");
    }

    #[rstest::rstest]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_selecting_any_map_synthetic_id_projects_whole_map(
        #[case] _selected_id: i32,
    ) {
        // prune({map_id}), prune({key_id}), prune({value_id}) all return the schema with
        // only the map field (full key + value types preserved).
        unimplemented!("avro::schema_util::prune_columns map id");
    }

    #[test]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_selecting_struct_parent_returns_empty_struct_shell() {
        // prune({struct_id}) returns schema with the struct field but an empty body.
        unimplemented!("avro::schema_util::prune_columns struct parent");
    }

    #[test]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_selecting_struct_child_returns_struct_with_only_child() {
        // prune({struct_child_id}) returns schema where the struct keeps only the selected child.
        unimplemented!("avro::schema_util::prune_columns struct child");
    }

    #[rstest::rstest]
    #[case(8)]
    #[case(9)]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_selecting_any_list_synthetic_id_projects_whole_list(
        #[case] _selected_id: i32,
    ) {
        // prune({list_id}) and prune({element_id}) both return the schema with the list field intact.
        unimplemented!("avro::schema_util::prune_columns list id");
    }

    #[test]
    #[ignore = "no avro::schema_util::prune_columns"]
    fn test_prune_columns_selecting_variant_projects_whole_variant_field() {
        // prune({variant_field_id}) returns schema with the variant field intact.
        unimplemented!("avro::schema_util::prune_columns variant");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `mapping::NameMapping` module.
    //
    // Eventual surface:
    //   mapping::MappedField { id: i32, names: Vec<String>, nested: Option<MappedFields> }
    //   mapping::MappedFields(Vec<MappedField>) — rejects top-level duplicate ids/names at construction
    //   mapping::NameMapping::create(&Schema) -> NameMapping
    //   NameMapping::find(id: i32) -> Option<&MappedField>           // recurses through tree
    //   NameMapping::find_path(path: &[&str]) -> Option<&MappedField> // top-level only without path
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_flat_schema_records_each_top_level_field() {
        // NameMapping::create on a flat schema yields one MappedField per top-level column,
        // each with the field's id and a single-element names vec.
        unimplemented!("mapping::NameMapping flat");
    }

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_nested_struct_schema_carries_child_tree() {
        // A struct field maps to a MappedField whose `nested` carries one MappedField per child.
        unimplemented!("mapping::NameMapping nested struct");
    }

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_map_schema_uses_synthetic_key_value_names() {
        // Map<string,int> maps to a MappedField whose nested children are named "key" and "value"
        // with the map's synthetic key/value ids.
        unimplemented!("mapping::NameMapping map");
    }

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_map_of_struct_key_preserves_inner_struct_tree() {
        // Map with a struct key: the synthetic "key" MappedField carries the struct's child tree.
        unimplemented!("mapping::NameMapping complex key");
    }

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_map_of_struct_value_preserves_inner_struct_tree() {
        // Map with a struct value: the synthetic "value" MappedField carries the struct's child tree.
        unimplemented!("mapping::NameMapping complex value");
    }

    #[test]
    #[ignore = "no mapping::NameMapping"]
    fn test_name_mapping_from_list_schema_uses_synthetic_element_name() {
        // List<T> maps to a MappedField with a single child named "element" carrying T's id.
        unimplemented!("mapping::NameMapping list");
    }

    #[test]
    #[ignore = "no Schema duplicate-id rejection at construction"]
    fn test_schema_construction_rejects_duplicate_field_ids() {
        // Schema::builder with two fields sharing an id raises at build().
        unimplemented!("Schema duplicate id rejection");
    }

    #[test]
    #[ignore = "no MappedFields top-level duplicate-name rejection"]
    fn test_mapped_fields_constructor_rejects_duplicate_top_level_names() {
        // MappedFields::new with two MappedField entries sharing a name rejects.
        unimplemented!("MappedFields duplicate name rejection");
    }

    #[test]
    #[ignore = "no mapping::NameMapping nested-context duplicates accepted"]
    fn test_mapped_fields_allows_duplicate_names_in_separate_contexts() {
        // Same name appearing under different parents (or under different map/list synthetic
        // contexts) is accepted; only duplicates within a single MappedFields list are rejected.
        unimplemented!("MappedFields nested-context duplicates");
    }

    #[test]
    #[ignore = "no mapping::NameMapping::find(id)"]
    fn test_name_mapping_find_by_id_walks_full_tree() {
        // find(id) returns the matching MappedField for any id at any depth: top-level, inside a
        // nested struct, inside a map value's struct, inside a list element's struct.
        unimplemented!("NameMapping::find by id");
    }

    #[test]
    #[ignore = "no mapping::NameMapping::find_path"]
    fn test_name_mapping_find_by_top_level_name_only_with_synthetic_names_requiring_paths() {
        // find("element"), find("key"), find("value") return None at the top level; synthetic
        // names require a path argument. Top-level user-supplied names resolve directly.
        unimplemented!("NameMapping::find by name");
    }

    #[test]
    #[ignore = "no mapping::NameMapping Variant leaf"]
    fn test_name_mapping_with_variant_field_is_treated_as_a_leaf() {
        // Variant fields map as a leaf MappedField with no nested children (no introspection).
        unimplemented!("NameMapping variant");
    }

    #[test]
    #[ignore = "no NameMappingWithAvroSchema visitor (avro->iceberg + paired-walk into MappedFields)"]
    fn test_name_mapping_derived_from_avro_schema_handles_union_branches_and_named_records() {
        // Given an Avro record with: plain INT, plain STRING, nested record `location` with two
        // DOUBLE fields, an array of STRING, a 2-branch union [NULL, STRING], and a 6-branch
        // union [NULL, STRING, innerRecord1{lat,long}, innerRecord2{lat,long},
        //        innerRecord3{innerUnion=[STRING,INT]}, enum timezone, fixed bitmap]:
        //
        // After AvroSchemaUtil::to_iceberg the resulting Iceberg Schema is walked alongside
        // the original Avro schema by AvroWithPartnerByStructureVisitor + NameMappingWithAvroSchema,
        // and yields a MappedFields tree where:
        //   - top-level Avro field names map to fresh top-level ids
        //   - union branches lend their type-tag names (`string`, `int`, named records,
        //     fixed name, enum name) as MappedField names inside the union's nested context
        //   - array elements get a synthetic `element` MappedField id
        //   - nested records appear as nested MappedFields trees
        unimplemented!("avro NameMapping visitor");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `avro::schema_convert` bridge.
    //
    // Eventual surface:
    //   avro::schema_convert::to_avro(&Schema)   -> avro::Schema
    //   avro::schema_convert::from_avro(&avro::Schema) -> Schema
    // Rust uses apache_avro directly in `iceberg-rust/src/table/manifest.rs`
    // but has no Iceberg-side schema-bridge facade.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_primitive_types_round_trip_through_avro_schema() {
        // The 15 spec primitives (Boolean, Int, Long, Float, Double, Date, Time, Timestamp
        // with/without zone, String, Uuid, Fixed, Binary, Decimal, Variant) survive the
        // Iceberg <-> Avro round trip and compare equal to the original.
        unimplemented!("avro::schema_convert primitives");
    }

    #[test]
    #[ignore = "no avro::schema_convert reverse-direction timestamp handling"]
    fn test_avro_timestamp_micros_without_adjust_to_utc_decodes_as_timestamp_without_zone() {
        // An Avro `timestamp-micros` type with no `adjust-to-utc` property decodes to
        // Iceberg `Timestamp` (without zone). Forward direction is lossy (Iceberg always
        // emits the zone-bearing flag); reverse direction is unsupported.
        unimplemented!("avro::schema_convert timestamp reverse");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_struct_with_all_primitives_round_trips_via_named_record() {
        // A top-level struct containing all 15 primitives round-trips as a named Avro record.
        unimplemented!("avro::schema_convert struct");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_list_round_trips_with_element_id_prop_on_avro_side() {
        // List<Uuid> round-trips and the avro array carries an `element-id` prop matching
        // the Iceberg element_id.
        unimplemented!("avro::schema_convert list");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_list_of_structs_uses_inner_named_record() {
        // List<Struct{lat,long}> round-trips with the inner struct emitted as a named record.
        unimplemented!("avro::schema_convert list of struct");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_map_with_non_string_key_round_trips_via_array_of_pair_encoding() {
        // Map<Long, Binary> round-trips through the Avro `array of {key, value}` encoding
        // because Avro map keys must be strings.
        unimplemented!("avro::schema_convert array-of-pair map");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_map_with_string_key_round_trips_via_native_avro_map() {
        // Map<String, Binary> round-trips as a native Avro map; key-id and value-id props
        // appear on the avro map.
        unimplemented!("avro::schema_convert native avro map");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_map_of_list_to_struct_round_trips_via_array_of_pair_encoding() {
        // Map<List<T>, Struct{...}> uses array-of-pair encoding because the key is non-string.
        unimplemented!("avro::schema_convert array-of-pair map nested");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_iceberg_map_of_string_to_struct_round_trips_via_native_avro_map() {
        // Map<String, Struct{...}> uses the native Avro map encoding.
        unimplemented!("avro::schema_convert native avro map of struct");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_complex_arbitrarily_nested_iceberg_schema_produces_printable_avro_schema() {
        // An arbitrarily nested schema (lists, maps, structs, primitives interleaved several
        // levels deep) round-trips and the avro side serialises to a non-empty JSON string.
        unimplemented!("avro::schema_convert complex");
    }

    #[test]
    #[ignore = "no avro::schema_convert"]
    fn test_special_character_field_names_are_sanitised_with_original_preserved_in_prop() {
        // Field names "9x", "a.b", "☃", "a#b" become Avro-valid identifiers ("_9x",
        // "a_x2Eb", "_x2603", "a_x23b") and the original name is preserved in the
        // `iceberg-field-name` Avro field prop.
        unimplemented!("avro::schema_convert special chars");
    }

    #[test]
    #[ignore = "no avro::schema_convert doc propagation"]
    fn test_field_docs_are_preserved_across_avro_round_trip() {
        // Per-field docs survive both directions of the round trip.
        unimplemented!("avro::schema_convert doc");
    }

    #[test]
    #[ignore = "no avro::schema_convert Variant encoding"]
    fn test_variant_field_encodes_as_id_named_record_with_metadata_and_value_bytes_children() {
        // Variant fields encode as an Avro record named "rN" (id-named) whose children are
        // `metadata: bytes` and `value: bytes`.
        unimplemented!("avro::schema_convert variant");
    }

    #[test]
    #[ignore = "no avro schema id-detection helpers in iceberg-rust-spec: needs remove_ids(&Schema) -> avro::Schema and has_ids(&avro::Schema) -> bool that walks every nested level"]
    fn test_avro_field_id_detection_walks_to_every_nesting_level() {
        // Behaviour to pin once the helpers exist:
        //   1. Build an Iceberg schema with a top-level Long, a Map<String,
        //      Struct<Float, Float>>, a List<String>, and a Variant.
        //   2. remove_ids(&schema) -> avro::Schema with no `field-id` props anywhere.
        //   3. has_ids(&avro_schema) == false on the fresh result.
        //   4. Adding a `field-id` prop to the first top-level avro field flips
        //      has_ids to true.
        //   5. Starting fresh and adding `field-id` deep inside (map value -> inner
        //      struct's optional Float field) also flips has_ids to true -- i.e. the
        //      walk descends through union, map-value, and nested struct branches.
        unimplemented!("avro::schema_id_util not implemented");
    }
}
