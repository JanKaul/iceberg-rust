use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogList as DFCatalogList, CatalogProvider};
use futures::{stream, StreamExt};
use iceberg_rust::catalog::CatalogList;

use crate::error::Error;

use super::catalog::IcebergCatalog;

pub struct IcebergCatalogList(DashMap<String, Arc<dyn CatalogProvider>>);

impl IcebergCatalogList {
    pub async fn new(catalog_list: Arc<dyn CatalogList>) -> Result<Self, Error> {
        let catalogs = catalog_list.list_catalogs().await;

        let map = stream::iter(catalogs.into_iter())
            .then(|x| {
                let catalog_list = catalog_list.clone();
                async move {
                    let catalog = catalog_list.catalog(&x).await?;
                    Some((
                        x,
                        Arc::new(IcebergCatalog::new(catalog, None).await.ok()?)
                            as Arc<dyn CatalogProvider>,
                    ))
                }
            })
            .filter_map(|x| async move { x })
            .collect::<DashMap<_, _>>()
            .await;

        Ok(IcebergCatalogList(map))
    }
}

impl DFCatalogList for IcebergCatalogList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.0.get(name).as_deref().cloned()
    }

    fn catalog_names(&self) -> Vec<String> {
        self.0.iter().map(|c| c.key().clone()).collect()
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.0.insert(name, catalog)
    }
}
