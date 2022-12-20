use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    error::Result,
};
use iceberg_rust::catalog::{namespace::Namespace, Catalog};

use crate::{mirror::Mirror, schema::IcebergSchema};

pub struct IcebergCatalog {
    catalog: Arc<Mirror>,
}

impl IcebergCatalog {
    pub async fn new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        Ok(IcebergCatalog {
            catalog: Arc::new(Mirror::new(catalog).await?),
        })
    }
}

impl CatalogProvider for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        let namespaces = self.catalog.schema_names(None);
        match namespaces {
            Err(_) => vec![],
            Ok(namespaces) => namespaces.into_iter().map(|x| x.to_string()).collect(),
        }
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let namespaces = self.schema_names();
        namespaces.iter().find(|x| *x == name).and_then(|y| {
            Some(Arc::new(IcebergSchema::new(
                Namespace::try_new(&y.split(".").map(|z| z.to_owned()).collect::<Vec<String>>())
                    .ok()?,
                Arc::clone(&self.catalog),
            )) as Arc<dyn SchemaProvider>)
        })
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use iceberg_catalog_rest_client::{apis::configuration::Configuration, catalog::RestCatalog};
    use iceberg_rust::{
        catalog::Catalog,
        object_store::{aws::AmazonS3Builder, ObjectStore},
    };

    use datafusion::{
        arrow::{array, record_batch::RecordBatch},
        prelude::*,
    };

    use super::IcebergCatalog;

    fn configuration() -> Configuration {
        Configuration {
            base_path: "http://localhost:8080".to_string(),
            user_agent: None,
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
        }
    }

    #[tokio::test]
    pub async fn test_catalog() {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("No access key provided.");
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").expect("No secret key provided.");

        let object_store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_region("eu-central-1")
                .with_bucket_name("dashbook-arrow-testing")
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .build()
                .expect("Failed to create aws object store"),
        );

        let catalog: Arc<dyn Catalog> = Arc::new(RestCatalog::new(
            "my_catalog".to_owned(),
            configuration(),
            object_store,
        ));

        let datafusion_catalog = Arc::new(
            IcebergCatalog::new(catalog)
                .await
                .expect("Failed to create iceberg catalog"),
        );

        let ctx = SessionContext::new();

        ctx.register_catalog("my_catalog", datafusion_catalog);

        let df = ctx
            .sql("SELECT county, SUM(cases) FROM my_catalog.dashbook.covid_nyt GROUP BY county")
            .await
            .expect("Failed to create dataframe.");

        // execute the plan
        let results: Vec<RecordBatch> = df.collect().await.expect("Failed to execute query plan.");

        let batch = results
            .into_iter()
            .find(|batch| batch.num_rows() > 0)
            .expect("All record batches are empty");

        let cases = batch
            .column(1)
            .as_any()
            .downcast_ref::<array::Int64Array>()
            .expect("Failed to get values from batch.");

        // Value can either be 0.9 or 1.8
        assert!(cases.value(0) > 100)
    }
}
