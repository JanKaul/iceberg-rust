/*!
Defining the [ViewBuilder] struct for creating catalog views and starting create/replace transactions
*/

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use object_store::path::Path;
use uuid::Uuid;

use crate::catalog::identifier::Identifier;
use crate::catalog::relation::Relation;
use crate::error::Error;
use crate::spec::schema::Schema;
use crate::spec::view_metadata::{VersionBuilder, ViewMetadataBuilder, ViewRepresentation};

use super::Catalog;
use super::View;

///Builder pattern to create a view
pub struct ViewBuilder {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: ViewMetadataBuilder,
}

impl Deref for ViewBuilder {
    type Target = ViewMetadataBuilder;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for ViewBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl ViewBuilder {
    /// Creates a new [TableBuilder] to create a Metastore view with some default metadata entries already set.
    pub fn new(
        sql: impl ToString,
        schema: Schema,
        identifier: impl ToString,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, Error> {
        let identifier = Identifier::parse(&identifier.to_string())?;
        let mut builder = ViewMetadataBuilder::default();
        builder
            .with_schema((1, schema))
            .with_version((
                1,
                VersionBuilder::default()
                    .version_id(1)
                    .with_representation(ViewRepresentation::Sql {
                        sql: sql.to_string(),
                        dialect: "ANSI".to_string(),
                    })
                    .schema_id(1)
                    .build()?,
            ))
            .current_version_id(1);
        Ok(ViewBuilder {
            metadata: builder,
            identifier,
            catalog,
        })
    }
    /// Building a table writes the metadata file and commits the table to either the metastore or the filesystem
    pub async fn build(self) -> Result<View, Error> {
        let object_store = self.catalog.object_store();
        let metadata = self.metadata.build()?;
        let location = &metadata.location;
        let uuid = Uuid::new_v4();
        let version = &metadata.current_version_id;
        let metadata_json = serde_json::to_string(&metadata)?;
        let path: Path = (location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json")
            .into();
        object_store.put(&path, metadata_json.into()).await?;
        if let Relation::View(view) = self
            .catalog
            .register_table(self.identifier, path.as_ref())
            .await?
        {
            Ok(view)
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
