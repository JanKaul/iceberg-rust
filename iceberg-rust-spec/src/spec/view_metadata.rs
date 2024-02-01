/*!
 * A Struct for the view metadata   
*/

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use crate::error::Error;

use super::schema::Schema;

use _serde::ViewMetadataEnum;

/// Prefix used to denote branch references in the view properties
pub static REF_PREFIX: &str = "ref-";

/// Fields for the version 1 of the view metadata.
pub type ViewMetadata = GeneralViewMetadata<Option<()>>;
/// Builder for the view metadata
pub type ViewMetadataBuilder = GeneralViewMetadataBuilder<Option<()>>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(try_from = "ViewMetadataEnum<T>", into = "ViewMetadataEnum<T>")]
/// Fields for the version 1 of the view metadata.
pub struct GeneralViewMetadata<T: Clone> {
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
    pub versions: HashMap<i64, Version>,
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
    pub materialization: T,
}

impl<T: Clone> GeneralViewMetadata<T> {
    /// Get current schema
    #[inline]
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        let id = self.current_version(branch)?.schema_id;
        self.schemas
            .get(&id)
            .ok_or_else(|| Error::InvalidFormat("view metadata".to_string()))
    }
    /// Get schema for version
    #[inline]
    pub fn schema(&self, version_id: i64) -> Result<&Schema, Error> {
        let id = self
            .versions
            .get(&version_id)
            .ok_or_else(|| Error::NotFound("Version".to_string(), version_id.to_string()))?
            .schema_id;
        self.schemas
            .get(&id)
            .ok_or_else(|| Error::InvalidFormat("view metadata".to_string()))
    }
    /// Get current version
    #[inline]
    pub fn current_version(&self, snapshot_ref: Option<&str>) -> Result<&Version, Error> {
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
    /// Add schema to view metadata
    #[inline]
    pub fn add_schema(&mut self, schema: Schema) {
        self.schemas.insert(schema.schema_id, schema);
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

    use super::{FormatVersion, GeneralViewMetadata, Version, VersionLogStruct};

    /// Metadata of an iceberg view
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataEnum<T> {
        /// Version 1 of the table metadata
        V1(ViewMetadataV1<T>),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 1 of the view metadata.
    pub struct ViewMetadataV1<T> {
        /// A UUID that identifies the view, generated when the view is created. Implementations must throw an exception if a view’s UUID does not match the expected UUID after refreshing metadata
        pub view_uuid: Uuid,
        /// An integer version number for the view format; must be 1
        pub format_version: VersionNumber<1>,
        /// The view’s base location. This is used to determine where to store manifest files and view metadata files.
        pub location: String,
        /// Current version of the view. Set to ‘1’ when the view is first created.
        pub current_version_id: i64,
        /// An array of structs describing the last known versions of the view. Controlled by the table property: “version.history.num-entries”. See section Versions.
        pub versions: Vec<Version>,
        /// A list of timestamp and version ID pairs that encodes changes to the current version for the view.
        /// Each time the current-version-id is changed, a new entry should be added with the last-updated-ms and the new current-version-id.
        pub version_log: Vec<VersionLogStruct>,
        /// A list of schemas, the same as the ‘schemas’ field from Iceberg table spec.
        pub schemas: Vec<SchemaV2>,
        /// A string to string map of view properties. This is used for metadata such as “comment” and for settings that affect view maintenance.
        /// This is not intended to be used for arbitrary metadata.
        pub properties: Option<HashMap<String, String>>,
        pub materialization: T,
    }

    impl<T: Clone> TryFrom<ViewMetadataEnum<T>> for GeneralViewMetadata<T> {
        type Error = Error;
        fn try_from(value: ViewMetadataEnum<T>) -> Result<Self, Self::Error> {
            match value {
                ViewMetadataEnum::V1(metadata) => metadata.try_into(),
            }
        }
    }

    impl<T: Clone> From<GeneralViewMetadata<T>> for ViewMetadataEnum<T> {
        fn from(value: GeneralViewMetadata<T>) -> Self {
            match value.format_version {
                FormatVersion::V1 => ViewMetadataEnum::V1(value.into()),
            }
        }
    }

    impl<T: Clone> TryFrom<ViewMetadataV1<T>> for GeneralViewMetadata<T> {
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
                materialization: value.materialization,
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

    impl<T: Clone> From<GeneralViewMetadata<T>> for ViewMetadataV1<T> {
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
                materialization: value.materialization,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 2 of the view metadata.
pub struct Version {
    /// Monotonically increasing id indicating the version of the view. Starts with 1.
    pub version_id: i64,
    /// ID of the schema for the view version
    pub schema_id: i32,
    #[builder(
        default = "SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64"
    )]
    /// Timestamp expressed in ms since epoch at which the version of the view was created.
    pub timestamp_ms: i64,
    #[builder(default)]
    /// A string map summarizes the version changes, including operation, described in Summary.
    pub summary: Summary,
    #[builder(setter(each(name = "with_representation")), default)]
    /// A list of “representations” as described in Representations.
    pub representations: Vec<ViewRepresentation>,
    #[builder(default)]
    /// A string specifying the catalog to use when the table or view references in the view definition do not contain an explicit catalog.
    pub default_catalog: Option<String>,
    #[builder(default)]
    /// The namespace to use when the table or view references in the view definition do not contain an explicit namespace.
    /// Since the namespace may contain multiple parts, it is serialized as a list of strings.
    pub default_namespace: Option<Vec<String>>,
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
    /// Name of the engine that created the view version
    pub engine_name: Option<String>,
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

impl Representation for ViewRepresentation {}

/// Marks a representation for an iceberg view
pub trait Representation: Clone {}

#[cfg(test)]
mod tests {

    use crate::{error::Error, spec::view_metadata::ViewMetadata};

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
}
