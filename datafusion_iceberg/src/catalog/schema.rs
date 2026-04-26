use std::{any::Any, ops::Deref, sync::Arc};

use datafusion::{catalog::SchemaProvider, datasource::TableProvider, error::Result};
use iceberg_rust::catalog::{identifier::Identifier, namespace::Namespace};

use crate::{catalog::mirror::Mirror, error::Error};

#[derive(Debug)]
pub struct IcebergSchema {
    schema: Namespace,
    catalog: Arc<Mirror>,
}

impl IcebergSchema {
    pub(crate) fn new(schema: Namespace, catalog: Arc<Mirror>) -> Self {
        IcebergSchema { schema, catalog }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for IcebergSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        let tables = self.catalog.table_names(&self.schema);
        match tables {
            Err(_) => vec![],
            Ok(schemas) => schemas.into_iter().map(|x| x.name().to_owned()).collect(),
        }
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.catalog
            .table(
                Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                    .map_err(Error::from)?,
            )
            .await
    }
    fn table_exist(&self, name: &str) -> bool {
        self.catalog.table_exists(
            Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                .unwrap(),
        )
    }
    fn register_table(
        &self,
        name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let identifier = Identifier::try_new(&[self.schema.deref(), &[name]].concat(), None)
            .map_err(Error::from)?;
        self.catalog.register_table(identifier)?;
        Ok(None)
    }
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let identifier =
            Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                .map_err(Error::from)?;
        self.catalog.deregister_table(identifier)?;
        Ok(None)
    }
}
