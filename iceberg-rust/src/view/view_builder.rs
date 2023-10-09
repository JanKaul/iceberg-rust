/*!
Defining the [ViewBuilder] struct for creating catalog views and starting create/replace transactions
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use object_store::path::Path;
use uuid::Uuid;

use crate::catalog::identifier::Identifier;
use crate::catalog::relation::Relation;
use crate::model::schema::SchemaV2;
use crate::model::view_metadata::{
    FormatVersion, Operation, Summary, Version, VersionLogStruct, ViewMetadata, ViewRepresentation,
};
use anyhow::{anyhow, Result};

use super::Catalog;
use super::View;

///Builder pattern to create a view
pub struct ViewBuilder {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: ViewMetadata,
}

impl ViewBuilder {
    /// Creates a new [TableBuilder] to create a Metastore view with some default metadata entries already set.
    pub fn new(
        sql: &str,
        base_path: &str,
        schema: SchemaV2,
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self> {
        let summary = Summary {
            operation: Operation::Create,
            engine_version: None,
        };
        let representation = ViewRepresentation::Sql {
            sql: sql.to_owned(),
            dialect: "ANSI".to_owned(),
            schema_id: None,
            default_catalog: None,
            default_namespace: None,
            field_aliases: None,
            field_docs: None,
        };
        let version = Version {
            version_id: 1,
            timestamp_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|err| anyhow!(err.to_string()))?
                .as_millis() as i64,
            summary,
            representations: vec![representation],
        };
        let version_log = vec![VersionLogStruct {
            timestamp_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|err| anyhow!(err.to_string()))?
                .as_millis() as i64,
            version_id: 1,
        }];
        let metadata = ViewMetadata {
            format_version: FormatVersion::V1,
            location: base_path.to_owned() + &identifier.to_string().replace('.', "/"),
            schemas: Some(HashMap::from_iter(vec![(1, schema.try_into()?)])),
            current_schema_id: Some(1),
            versions: HashMap::from_iter(vec![(1, version)]),
            current_version_id: 1,
            version_log,
            properties: None,
        };
        Ok(ViewBuilder {
            metadata,
            identifier,
            catalog,
        })
    }
    /// Building a table writes the metadata file and commits the table to either the metastore or the filesystem
    pub async fn commit(self) -> Result<View> {
        let object_store = self.catalog.object_store();
        let location = &self.metadata.location;
        let uuid = Uuid::new_v4();
        let version = &self.metadata.current_version_id;
        let metadata_json =
            serde_json::to_string(&self.metadata).map_err(|err| anyhow!(err.to_string()))?;
        let path: Path = (location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json")
            .into();
        object_store
            .put(&path, metadata_json.into())
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        if let Relation::View(view) = self
            .catalog
            .register_table(self.identifier, path.as_ref())
            .await?
        {
            Ok(view)
        } else {
            Err(anyhow!("Building the table failed because registering the table in the catalog didn't return a table."))
        }
    }
}
