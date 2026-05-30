//! View metadata implementation for Iceberg views
//!
//! This module contains the implementation of view metadata for Iceberg views, including:
//! - View metadata structure and versioning (V1)
//! - Schema management
//! - View versions and history
//! - View representations (SQL)
//! - Metadata properties and logging
//!
//! The view metadata format is defined in the [Iceberg View Spec](https://iceberg.apache.org/spec/#view-metadata)

use std::{
    collections::HashMap,
    fmt::{self},
    str,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_builder::Builder;
use derive_getters::Getters;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use crate::{error::Error, identifier::Identifier};

use super::{
    schema::{Schema, DEFAULT_SCHEMA_ID},
    tabular::TabularMetadataRef,
};

pub use _serde::ViewMetadataV1;

use _serde::ViewMetadataEnum;

/// Prefix used to denote branch references in the view properties
pub static REF_PREFIX: &str = "ref-";

/// Default version id
pub static DEFAULT_VERSION_ID: i64 = 0;

/// Fields for the version 1 of the view metadata.
pub type ViewMetadata = GeneralViewMetadata<Option<()>>;
/// Builder for the view metadata
pub type ViewMetadataBuilder = GeneralViewMetadataBuilder<Option<()>>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(try_from = "ViewMetadataEnum<T>", into = "ViewMetadataEnum<T>")]
/// Fields for the version 1 of the view metadata.
pub struct GeneralViewMetadata<T: Materialization> {
    #[builder(default = "Uuid::new_v4()")]
    /// A UUID that identifies the view, generated when the view is created. Implementations must throw an exception if a view’s UUID does not match the expected UUID after refreshing metadata
    pub view_uuid: Uuid,
    #[builder(default)]
    /// An integer version number for the view format; must be 1
    pub format_version: FormatVersion,
    #[builder(setter(into))]
    /// The view’s base location. This is used to determine where to store manifest files and view metadata files.
    pub location: String,
    /// Current version of the view. Set to ‘1’ when the view is first created.
    pub current_version_id: i64,
    #[builder(setter(each(name = "with_version")), default)]
    /// An array of structs describing the last known versions of the view. Controlled by the table property: “version.history.num-entries”. See section Versions.
    pub versions: HashMap<i64, Version<T>>,
    #[builder(default)]
    /// A list of timestamp and version ID pairs that encodes changes to the current version for the view.
    /// Each time the current-version-id is changed, a new entry should be added with the last-updated-ms and the new current-version-id.
    pub version_log: Vec<VersionLogStruct>,
    #[builder(setter(each(name = "with_schema")), default)]
    /// A list of schemas, the same as the ‘schemas’ field from Iceberg table spec.
    pub schemas: HashMap<i32, Schema>,
    #[builder(default)]
    /// A string to string map of view properties. This is used for metadata such as “comment” and for settings that affect view maintenance.
    /// This is not intended to be used for arbitrary metadata.
    pub properties: HashMap<String, String>,
}

impl<T: Materialization> GeneralViewMetadata<T> {
    /// Gets the current schema for a given branch, or the view's current schema if no branch is specified
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the schema for
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The current schema, or an error if the schema cannot be found
    #[inline]
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        let id = self.current_version(branch)?.schema_id;
        self.schemas
            .get(&id)
            .ok_or_else(|| Error::InvalidFormat("view metadata".to_string()))
    }

    /// Gets the schema for a specific version ID
    ///
    /// # Arguments
    /// * `version_id` - The ID of the version to get the schema for
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The schema for the version, or an error if the schema cannot be found
    #[inline]
    pub fn schema(&self, version_id: i64) -> Result<&Schema, Error> {
        let id = self
            .versions
            .get(&version_id)
            .ok_or_else(|| Error::NotFound(format!("View version {version_id}")))?
            .schema_id;
        self.schemas
            .get(&id)
            .ok_or_else(|| Error::InvalidFormat("view metadata".to_string()))
    }

    /// Gets the current version for a given reference, or the view's current version if no reference is specified
    ///
    /// # Arguments
    /// * `snapshot_ref` - Optional snapshot reference name to get the version for
    ///
    /// # Returns
    /// * `Result<&Version<T>, Error>` - The current version, or an error if the version cannot be found
    #[inline]
    pub fn current_version(&self, snapshot_ref: Option<&str>) -> Result<&Version<T>, Error> {
        let version_id: i64 = match snapshot_ref {
            None => self.current_version_id,
            Some(reference) => self
                .properties
                .get(&(REF_PREFIX.to_string() + reference))
                .and_then(|x| x.parse().ok())
                .unwrap_or(self.current_version_id),
        };
        self.versions
            .get(&version_id)
            .ok_or_else(|| Error::InvalidFormat("view metadata".to_string()))
    }

    /// Adds a new schema to the view metadata
    ///
    /// # Arguments
    /// * `schema` - The schema to add
    #[inline]
    pub fn add_schema(&mut self, schema: Schema) {
        self.schemas.insert(*schema.schema_id(), schema);
    }
}

impl ViewMetadata {
    pub fn as_ref(&self) -> TabularMetadataRef<'_> {
        TabularMetadataRef::View(self)
    }
}

impl fmt::Display for ViewMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for ViewMetadata {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

mod _serde {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{schema::SchemaV2, table_metadata::VersionNumber},
    };

    use super::{FormatVersion, GeneralViewMetadata, Materialization, Version, VersionLogStruct};

    /// Metadata of an iceberg view
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataEnum<T: Materialization> {
        /// Version 1 of the table metadata
        V1(ViewMetadataV1<T>),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 1 of the view metadata.
    pub struct ViewMetadataV1<T: Materialization> {
        /// A UUID that identifies the view, generated when the view is created. Implementations must throw an exception if a view’s UUID does not match the expected UUID after refreshing metadata
        pub view_uuid: Uuid,
        /// An integer version number for the view format; must be 1
        pub format_version: VersionNumber<1>,
        /// The view’s base location. This is used to determine where to store manifest files and view metadata files.
        pub location: String,
        /// Current version of the view. Set to ‘1’ when the view is first created.
        pub current_version_id: i64,
        /// An array of structs describing the last known versions of the view. Controlled by the table property: “version.history.num-entries”. See section Versions.
        pub versions: Vec<Version<T>>,
        /// A list of timestamp and version ID pairs that encodes changes to the current version for the view.
        /// Each time the current-version-id is changed, a new entry should be added with the last-updated-ms and the new current-version-id.
        pub version_log: Vec<VersionLogStruct>,
        /// A list of schemas, the same as the ‘schemas’ field from Iceberg table spec.
        pub schemas: Vec<SchemaV2>,
        /// A string to string map of view properties. This is used for metadata such as “comment” and for settings that affect view maintenance.
        /// This is not intended to be used for arbitrary metadata.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub properties: Option<HashMap<String, String>>,
    }

    impl<T: Materialization> TryFrom<ViewMetadataEnum<T>> for GeneralViewMetadata<T> {
        type Error = Error;
        fn try_from(value: ViewMetadataEnum<T>) -> Result<Self, Self::Error> {
            match value {
                ViewMetadataEnum::V1(metadata) => metadata.try_into(),
            }
        }
    }

    impl<T: Materialization> From<GeneralViewMetadata<T>> for ViewMetadataEnum<T> {
        fn from(value: GeneralViewMetadata<T>) -> Self {
            match value.format_version {
                FormatVersion::V1 => ViewMetadataEnum::V1(value.into()),
            }
        }
    }

    impl<T: Materialization> TryFrom<ViewMetadataV1<T>> for GeneralViewMetadata<T> {
        type Error = Error;
        fn try_from(value: ViewMetadataV1<T>) -> Result<Self, Self::Error> {
            Ok(GeneralViewMetadata {
                view_uuid: value.view_uuid,
                format_version: FormatVersion::V1,
                location: value.location,
                current_version_id: value.current_version_id,
                versions: HashMap::from_iter(value.versions.into_iter().map(|x| (x.version_id, x))),
                version_log: value.version_log,
                properties: value.properties.unwrap_or_default(),
                schemas: HashMap::from_iter(
                    value
                        .schemas
                        .into_iter()
                        .map(|x| Ok((x.schema_id, x.try_into()?)))
                        .collect::<Result<Vec<_>, Error>>()?,
                ),
            })
        }
    }

    impl<T: Materialization> From<GeneralViewMetadata<T>> for ViewMetadataV1<T> {
        fn from(value: GeneralViewMetadata<T>) -> Self {
            ViewMetadataV1 {
                view_uuid: value.view_uuid,
                format_version: VersionNumber::<1>,
                location: value.location,
                current_version_id: value.current_version_id,
                versions: value.versions.into_values().collect(),
                version_log: value.version_log,
                properties: if value.properties.is_empty() {
                    None
                } else {
                    Some(value.properties)
                },
                schemas: value.schemas.into_values().map(Into::into).collect(),
            }
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Iceberg format version
#[derive(Default)]
pub enum FormatVersion {
    /// Iceberg spec version 1
    #[default]
    V1 = b'1',
}

impl TryFrom<u8> for FormatVersion {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(FormatVersion::V1),
            _ => Err(Error::Conversion(
                "u8".to_string(),
                "format version".to_string(),
            )),
        }
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => b'1',
        }
    }
}

impl From<FormatVersion> for i32 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder, Getters)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 2 of the view metadata.
pub struct Version<T: Materialization> {
    /// Monotonically increasing id indicating the version of the view. Starts with 1.
    #[builder(default = "DEFAULT_VERSION_ID")]
    pub version_id: i64,
    /// ID of the schema for the view version
    #[builder(default = "DEFAULT_SCHEMA_ID")]
    pub schema_id: i32,
    #[builder(
        default = "SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64"
    )]
    /// Timestamp expressed in ms since epoch at which the version of the view was created.
    pub timestamp_ms: i64,
    #[builder(default)]
    /// A string map summarizes the version changes, including operation, described in Summary.
    pub summary: Summary,
    #[builder(setter(each(name = "with_representation")), default)]
    /// A list of “representations” as described in Representations.
    pub representations: Vec<ViewRepresentation>,
    #[builder(setter(strip_option), default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// A string specifying the catalog to use when the table or view references in the view definition do not contain an explicit catalog.
    pub default_catalog: Option<String>,
    #[builder(setter(strip_option), default)]
    /// The namespace to use when the table or view references in the view definition do not contain an explicit namespace.
    /// Since the namespace may contain multiple parts, it is serialized as a list of strings.
    pub default_namespace: Vec<String>,
    /// Full identifier record of the storage table
    #[builder(default)]
    #[serde(skip_serializing_if = "Materialization::is_none")]
    pub storage_table: T,
}

pub trait Materialization: Clone + Default {
    fn is_none(&self) -> bool;
}

impl Materialization for Option<()> {
    fn is_none(&self) -> bool {
        true
    }
}

impl Materialization for Identifier {
    fn is_none(&self) -> bool {
        false
    }
}

impl<T: Materialization> Version<T> {
    pub fn builder() -> VersionBuilder<T> {
        VersionBuilder::default()
    }
}

impl<T: Materialization + Serialize> fmt::Display for Version<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl<T: Materialization + for<'de> Deserialize<'de>> str::FromStr for Version<T> {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 2 of the view metadata.
pub struct VersionLogStruct {
    /// The timestamp when the referenced version was made the current version
    pub timestamp_ms: i64,
    /// Version id of the view
    pub version_id: i64,
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// View operation that create the metadata file
#[derive(Default)]
pub enum Operation {
    /// Create view
    #[default]
    Create,
    /// Replace view
    Replace,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use Operation::*;
        match self {
            Create => serializer.serialize_str("create"),
            Replace => serializer.serialize_str("replace"),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "create" {
            Ok(Operation::Create)
        } else if s == "replace" {
            Ok(Operation::Replace)
        } else {
            Err(serde::de::Error::custom("Invalid view operation."))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 2 of the view metadata.
pub struct Summary {
    /// A string value indicating the view operation that caused this metadata to be created. Allowed values are “create” and “replace”.
    pub operation: Operation,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Name of the engine that created the view version
    pub engine_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// A string value indicating the version of the engine that performed the operation
    pub engine_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Fields for the version 2 of the view metadata.
pub enum ViewRepresentation {
    #[serde(rename = "sql")]
    /// This type of representation stores the original view definition in SQL and its SQL dialect.
    Sql {
        /// A string representing the original view definition in SQL
        sql: String,
        /// A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect.
        dialect: String,
    },
}

impl ViewRepresentation {
    pub fn sql(sql: &str, dialect: Option<&str>) -> Self {
        ViewRepresentation::Sql {
            sql: sql.to_owned(),
            dialect: dialect.unwrap_or("ansi").to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        error::Error,
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, Type},
            view_metadata::{Operation, ViewMetadata, ViewRepresentation},
        },
    };

    #[test]
    fn test_deserialize_view_data_v1() -> Result<(), Error> {
        let data = r#"
        {
        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
        "format-version" : 1,
        "location" : "s3://bucket/warehouse/default.db/event_agg",
        "current-version-id" : 1,
        "properties" : {
            "comment" : "Daily event counts"
        },
        "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
            "operation" : "create",
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
            },
            "representations" : [ {
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
            } ]
        } ],
        "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
            }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
            } ]
        } ],
        "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
        } ]
        }
        "#;
        let metadata =
            serde_json::from_str::<ViewMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: ViewMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    // --- ViewMetadata behaviour ----------------------------------------

    /// A multi-version view used as input to the behaviour tests below.
    /// Schema 10 is referenced by version 3 (the `staging` ref); schema 20
    /// by version 5 (the current version).
    fn sample_view_metadata_json() -> &'static str {
        r#"{
            "view-uuid": "11111111-2222-3333-4444-555555555555",
            "format-version": 1,
            "location": "s3://bucket/warehouse/db/v",
            "current-version-id": 5,
            "properties": {
                "comment": "rolling user counts",
                "ref-staging": "3"
            },
            "schemas": [
                {
                    "schema-id": 10,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "n", "required": true, "type": "int" }
                    ]
                },
                {
                    "schema-id": 20,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "n",     "required": true,  "type": "long" },
                        { "id": 2, "name": "label", "required": false, "type": "string" }
                    ]
                }
            ],
            "versions": [
                {
                    "version-id": 3,
                    "schema-id": 10,
                    "timestamp-ms": 1700000000000,
                    "default-namespace": ["staging"],
                    "summary": { "operation": "create" },
                    "representations": [
                        { "type": "sql", "sql": "select 1", "dialect": "ansi" }
                    ]
                },
                {
                    "version-id": 5,
                    "schema-id": 20,
                    "timestamp-ms": 1730000000000,
                    "default-namespace": ["prod"],
                    "summary": { "operation": "replace" },
                    "representations": [
                        { "type": "sql", "sql": "select n, label from t", "dialect": "ansi" }
                    ]
                }
            ],
            "version-log": [
                { "timestamp-ms": 1700000000000, "version-id": 3 },
                { "timestamp-ms": 1730000000000, "version-id": 5 }
            ]
        }"#
    }

    #[test]
    fn test_view_metadata_display_and_fromstr_round_trip() {
        let metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        let rendered = metadata.to_string();
        let parsed: ViewMetadata = rendered.parse().unwrap();
        assert_eq!(parsed, metadata);
    }

    #[test]
    fn test_view_metadata_current_schema_follows_current_version_id() {
        let metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        let schema = metadata.current_schema(None).unwrap();
        // current_version_id = 5 -> schema-id 20 -> two fields.
        assert_eq!(schema.schema_id(), &20);
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_view_metadata_current_version_via_branch_reference_property() {
        // properties["ref-staging"] = "3" routes the named ref to version 3.
        let metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        let version = metadata.current_version(Some("staging")).unwrap();
        assert_eq!(version.version_id, 3);
    }

    #[test]
    fn test_view_metadata_current_version_falls_back_when_ref_property_missing() {
        let metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        // No `ref-experimental` property -> fall back to current_version_id (5).
        let version = metadata.current_version(Some("experimental")).unwrap();
        assert_eq!(version.version_id, 5);
    }

    #[test]
    fn test_view_metadata_schema_lookup_for_unknown_version_id_returns_not_found() {
        let metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        let err = metadata.schema(9999).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn test_view_metadata_add_schema_inserts_by_schema_id() {
        let mut metadata: ViewMetadata = serde_json::from_str(sample_view_metadata_json()).unwrap();
        assert_eq!(metadata.schemas.len(), 2);

        let new_schema = Schema::builder()
            .with_schema_id(99)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();
        metadata.add_schema(new_schema.clone());

        assert_eq!(metadata.schemas.len(), 3);
        assert_eq!(metadata.schemas.get(&99), Some(&new_schema));
    }

    #[test]
    fn test_view_operation_serializes_as_lowercase_keyword() {
        assert_eq!(
            serde_json::to_string(&Operation::Create).unwrap(),
            "\"create\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::Replace).unwrap(),
            "\"replace\""
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"replace\"").unwrap(),
            Operation::Replace,
        );
        // Anything else is rejected by the custom Deserialize impl.
        assert!(serde_json::from_str::<Operation>("\"merge\"").is_err());
    }

    #[test]
    fn test_view_representation_sql_helper_defaults_dialect_to_ansi() {
        // Default dialect when caller passes None.
        let ViewRepresentation::Sql { sql, dialect } = ViewRepresentation::sql("select 1", None);
        assert_eq!(sql, "select 1");
        assert_eq!(dialect, "ansi");

        // Explicit dialect overrides the default.
        let ViewRepresentation::Sql { dialect, .. } =
            ViewRepresentation::sql("select 1", Some("spark"));
        assert_eq!(dialect, "spark");
    }

    // =====================================================================
    // Cycle L1: TestViewMetadataParser port — Java's view metadata JSON
    // parser test class. Rust's parser validates required fields via serde,
    // restricts format-version to V1 via the `VersionNumber<1>` newtype,
    // and lifts V1-shaped JSON into a `GeneralViewMetadata` with HashMaps
    // for versions/schemas keyed by id. Tests use distinctive UUIDs and
    // SQL fixtures (no upstream identifiers).
    // =====================================================================

    /// Helper: a fully-populated V1 view metadata JSON fixture for L1 tests.
    /// All required spec fields are present; properties + version-log carry
    /// non-empty payloads to verify lossless round-trip.
    fn full_l1_v1_json() -> &'static str {
        r#"{
            "view-uuid": "aaaa1111-bbbb-2222-cccc-3333dddd4444",
            "format-version": 1,
            "location": "s3://parity/test/views/order_summary",
            "current-version-id": 7,
            "properties": {
                "comment": "L1 fixture",
                "retention-days": "30"
            },
            "schemas": [
                {
                    "schema-id": 12,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "order_id", "required": true,  "type": "long" },
                        { "id": 2, "name": "amount",   "required": false, "type": "double" }
                    ]
                }
            ],
            "versions": [
                {
                    "version-id": 7,
                    "schema-id": 12,
                    "timestamp-ms": 1740000000000,
                    "default-namespace": ["sales", "reporting"],
                    "default-catalog": "prod",
                    "summary": {
                        "operation": "create",
                        "engine-name": "L1Engine"
                    },
                    "representations": [
                        {
                            "type": "sql",
                            "sql": "SELECT order_id, SUM(amount) FROM orders GROUP BY 1",
                            "dialect": "ansi"
                        }
                    ]
                }
            ],
            "version-log": [
                { "timestamp-ms": 1740000000000, "version-id": 7 }
            ]
        }"#
    }

    /// Valid V1 metadata parses with every required field accessible.
    #[test]
    fn test_view_metadata_parser_full_v1_round_trip_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        assert_eq!(metadata.current_version_id, 7);
        assert_eq!(metadata.versions.len(), 1);
        assert_eq!(metadata.schemas.len(), 1);
        assert_eq!(metadata.location, "s3://parity/test/views/order_summary");
        assert_eq!(
            metadata.properties.get("retention-days"),
            Some(&"30".to_string()),
        );

        let again: ViewMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(again, metadata);
    }

    /// Parser rejects JSON missing `format-version`.
    #[test]
    fn test_view_metadata_parser_rejects_missing_format_version_per_java() {
        let json = r#"{
            "view-uuid": "aaaa1111-bbbb-2222-cccc-3333dddd4444",
            "location": "s3://x/v",
            "current-version-id": 1,
            "schemas": [],
            "versions": [],
            "version-log": []
        }"#;
        assert!(serde_json::from_str::<ViewMetadata>(json).is_err());
    }

    /// Parser rejects `format-version` ≠ 1.
    #[test]
    fn test_view_metadata_parser_rejects_unsupported_format_version_per_java() {
        let json = r#"{
            "view-uuid": "aaaa1111-bbbb-2222-cccc-3333dddd4444",
            "format-version": 2,
            "location": "s3://x/v",
            "current-version-id": 1,
            "schemas": [],
            "versions": [],
            "version-log": []
        }"#;
        let err = serde_json::from_str::<ViewMetadata>(json);
        assert!(
            err.is_err(),
            "Rust only supports view format-version 1; got {err:?}",
        );
    }

    /// Parser rejects JSON missing `view-uuid`.
    #[test]
    fn test_view_metadata_parser_rejects_missing_view_uuid_per_java() {
        let json = r#"{
            "format-version": 1,
            "location": "s3://x/v",
            "current-version-id": 1,
            "schemas": [],
            "versions": [],
            "version-log": []
        }"#;
        assert!(serde_json::from_str::<ViewMetadata>(json).is_err());
    }

    /// Parser rejects JSON missing `current-version-id`.
    #[test]
    fn test_view_metadata_parser_rejects_missing_current_version_id_per_java() {
        let json = r#"{
            "view-uuid": "aaaa1111-bbbb-2222-cccc-3333dddd4444",
            "format-version": 1,
            "location": "s3://x/v",
            "schemas": [],
            "versions": [],
            "version-log": []
        }"#;
        assert!(serde_json::from_str::<ViewMetadata>(json).is_err());
    }

    /// Parser rejects JSON missing `location`.
    #[test]
    fn test_view_metadata_parser_rejects_missing_location_per_java() {
        let json = r#"{
            "view-uuid": "aaaa1111-bbbb-2222-cccc-3333dddd4444",
            "format-version": 1,
            "current-version-id": 1,
            "schemas": [],
            "versions": [],
            "version-log": []
        }"#;
        assert!(serde_json::from_str::<ViewMetadata>(json).is_err());
    }

    /// `version_log` entries round-trip preserving order and content.
    #[test]
    fn test_view_metadata_parser_preserves_version_log_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        assert_eq!(metadata.version_log.len(), 1);
        assert_eq!(metadata.version_log[0].version_id, 7);
        assert_eq!(metadata.version_log[0].timestamp_ms, 1_740_000_000_000);

        let reserialized = serde_json::to_string(&metadata).unwrap();
        let back: ViewMetadata = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(back.version_log, metadata.version_log);
    }

    /// Empty `properties` map either round-trips as empty or is omitted from
    /// the serialized output.
    #[test]
    fn test_view_metadata_parser_empty_properties_round_trip_per_java() {
        let json = r#"{
            "view-uuid": "11112222-3333-4444-5555-666677778888",
            "format-version": 1,
            "location": "s3://parity/test/views/empty_props",
            "current-version-id": 1,
            "schemas": [
                {
                    "schema-id": 1,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "x", "required": true, "type": "int" }
                    ]
                }
            ],
            "versions": [
                {
                    "version-id": 1,
                    "schema-id": 1,
                    "timestamp-ms": 1700000000000,
                    "default-namespace": ["root"],
                    "summary": { "operation": "create" },
                    "representations": [
                        { "type": "sql", "sql": "select 1", "dialect": "ansi" }
                    ]
                }
            ],
            "version-log": []
        }"#;
        let metadata: ViewMetadata = serde_json::from_str(json).unwrap();
        assert!(metadata.properties.is_empty());
        let reserialized = serde_json::to_string(&metadata).unwrap();
        let back: ViewMetadata = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(back, metadata);
    }

    /// Multiple representations per version are preserved.
    #[test]
    fn test_view_metadata_parser_multiple_representations_per_java() {
        let json = r#"{
            "view-uuid": "aaaabbbb-cccc-dddd-eeee-ffff00001111",
            "format-version": 1,
            "location": "s3://parity/test/views/multi_repr",
            "current-version-id": 1,
            "schemas": [
                {
                    "schema-id": 1,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "x", "required": true, "type": "int" }
                    ]
                }
            ],
            "versions": [
                {
                    "version-id": 1,
                    "schema-id": 1,
                    "timestamp-ms": 1700000000000,
                    "default-namespace": ["multi"],
                    "summary": { "operation": "create" },
                    "representations": [
                        { "type": "sql", "sql": "SELECT 1",  "dialect": "ansi" },
                        { "type": "sql", "sql": "SELECT x FROM t",  "dialect": "trino" }
                    ]
                }
            ],
            "version-log": []
        }"#;
        let metadata: ViewMetadata = serde_json::from_str(json).unwrap();
        let v = metadata.versions.get(&1).unwrap();
        assert_eq!(
            v.representations.len(),
            2,
            "all representations must round-trip",
        );

        let back: ViewMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        let v2 = back.versions.get(&1).unwrap();
        assert_eq!(v2.representations, v.representations);
    }

    /// Round-trip preserves `summary.engine-name`.
    #[test]
    fn test_view_metadata_parser_preserves_summary_engine_name_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        let v = metadata.versions.get(&7).unwrap();
        assert_eq!(v.summary.engine_name.as_deref(), Some("L1Engine"));

        let back: ViewMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(
            back.versions
                .get(&7)
                .unwrap()
                .summary
                .engine_name
                .as_deref(),
            Some("L1Engine"),
        );
    }

    /// `Operation::Create` and `Operation::Replace` are the only legal
    /// summary operations for views; unknown ones are rejected.
    #[test]
    fn test_view_metadata_summary_operation_strictness_per_java() {
        assert_eq!(
            serde_json::from_str::<Operation>("\"create\"").unwrap(),
            Operation::Create,
        );
        assert_eq!(
            serde_json::from_str::<Operation>("\"replace\"").unwrap(),
            Operation::Replace,
        );
        // Anything else must be rejected.
        for s in ["\"append\"", "\"drop\"", "\"Create\"", "\"\""] {
            assert!(
                serde_json::from_str::<Operation>(s).is_err(),
                "operation {s} must be rejected",
            );
        }
    }

    /// `default-namespace` accepts multi-segment paths.
    #[test]
    fn test_view_metadata_parser_multi_segment_default_namespace_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        let v = metadata.versions.get(&7).unwrap();
        assert_eq!(v.default_namespace, vec!["sales", "reporting"]);
    }

    /// `default-catalog` survives round-trip.
    #[test]
    fn test_view_metadata_parser_preserves_default_catalog_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        let v = metadata.versions.get(&7).unwrap();
        assert_eq!(v.default_catalog, Some("prod".to_string()));

        let back: ViewMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(
            back.versions.get(&7).unwrap().default_catalog,
            v.default_catalog
        );
    }

    /// Display+FromStr round-trip is equivalent to serde_json round-trip.
    #[test]
    fn test_view_metadata_display_fromstr_round_trip_per_java() {
        let metadata: ViewMetadata = serde_json::from_str(full_l1_v1_json()).unwrap();
        let rendered = metadata.to_string();
        let parsed: ViewMetadata = rendered.parse().unwrap();
        assert_eq!(parsed, metadata);
    }

    // ---------- Gaps pinned as #[ignore] ------------------------------------

    /// Java's parser rejects metadata where two `versions` entries share a
    /// `version-id`. Rust's parser collapses the Vec into a HashMap keyed by
    /// `version_id`, so the duplicate silently overwrites the earlier entry
    /// instead of producing an error.
    #[test]
    #[ignore = "feature gap: Rust converts versions Vec to HashMap, silently overwriting duplicate version-id rather than erroring at parse"]
    fn test_view_metadata_parser_rejects_duplicate_version_id_per_java() {}

    /// Java's parser rejects metadata where two `schemas` entries share a
    /// `schema-id`. Rust does the same silent collapse via HashMap.
    #[test]
    #[ignore = "feature gap: Rust converts schemas Vec to HashMap, silently overwriting duplicate schema-id rather than erroring at parse"]
    fn test_view_metadata_parser_rejects_duplicate_schema_id_per_java() {}

    /// Java's parser validates that `current-version-id` points to a
    /// version that actually exists in the `versions` array. Rust defers
    /// this check to the `current_version()` lookup method, so the parser
    /// happily accepts a dangling reference.
    #[test]
    #[ignore = "feature gap: Rust accepts dangling current-version-id at parse time; validation happens lazily on current_version() lookup"]
    fn test_view_metadata_parser_rejects_dangling_current_version_id_per_java() {}

    /// Java's parser validates that every version's `schema-id` is present
    /// in the `schemas` array. Rust defers this check to `schema(version_id)`
    /// lookup.
    #[test]
    #[ignore = "feature gap: Rust accepts dangling schema-id in version at parse time; validation happens lazily on schema() lookup"]
    fn test_view_metadata_parser_rejects_dangling_schema_id_in_version_per_java() {}
}
