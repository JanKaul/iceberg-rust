use async_trait::async_trait;
use futures::{FutureExt, TryFutureExt};
/**
Iceberg rest catalog implementation
*/
use iceberg_rust::{
    catalog::{
        commit::CommitView,
        create::{CreateMaterializedView, CreateTable, CreateView},
        identifier::{self, Identifier},
        namespace::Namespace,
        tabular::Tabular,
        Catalog, CatalogList,
    },
    error::Error,
    materialized_view::MaterializedView,
    object_store::{Bucket, ObjectStoreBuilder},
    spec::{
        identifier::FullIdentifier,
        materialized_view_metadata::MaterializedViewMetadata,
        table_metadata::TableMetadata,
        tabular::TabularMetadata,
        view_metadata::{self, ViewMetadata},
    },
    table::Table,
    view::View,
};
use object_store::ObjectStore;
use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    apis::{
        self,
        catalog_api_api::{self, NamespaceExistsError},
        configuration::Configuration,
    },
    models,
};

#[derive(Debug)]
pub struct RestCatalog {
    name: Option<String>,
    configuration: Configuration,
    object_store_builder: ObjectStoreBuilder,
}

impl RestCatalog {
    pub fn new(
        name: Option<&str>,
        configuration: Configuration,
        object_store_builder: ObjectStoreBuilder,
    ) -> Self {
        RestCatalog {
            name: name.map(ToString::to_string),
            configuration,
            object_store_builder,
        }
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    /// Catalog name
    fn name(&self) -> &str {
        self.name.as_ref().unwrap()
    }
    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, Error> {
        let response = catalog_api_api::create_namespace(
            &self.configuration,
            self.name.as_deref(),
            models::CreateNamespaceRequest {
                namespace: namespace.to_vec(),
                properties,
            },
        )
        .await
        .map_err(Into::<Error>::into)?;
        Ok(response.properties.unwrap_or_default())
    }
    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), Error> {
        catalog_api_api::drop_namespace(
            &self.configuration,
            self.name.as_deref(),
            &namespace.url_encode(),
        )
        .await
        .map_err(Into::<Error>::into)?;
        Ok(())
    }
    /// Load the namespace properties from the catalog
    async fn load_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<String, String>, Error> {
        let response = catalog_api_api::load_namespace_metadata(
            &self.configuration,
            self.name.as_deref(),
            &namespace.url_encode(),
        )
        .await
        .map_err(Into::<Error>::into)?;
        Ok(response.properties.unwrap_or_default())
    }
    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        namespace: &Namespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), Error> {
        catalog_api_api::update_properties(
            &self.configuration,
            self.name.as_deref(),
            &namespace.url_encode(),
            models::UpdateNamespacePropertiesRequest { updates, removals },
        )
        .await
        .map_err(Into::<Error>::into)?;
        Ok(())
    }
    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, Error> {
        match catalog_api_api::namespace_exists(
            &self.configuration,
            self.name.as_deref(),
            &namespace.url_encode(),
        )
        .await
        {
            Ok(()) => Ok(true),
            Err(err) => {
                if let apis::Error::ResponseError(err) = err {
                    if let Some(NamespaceExistsError::Status404(_)) = err.entity {
                        Ok(false)
                    } else {
                        Err(apis::Error::ResponseError(err).into())
                    }
                } else {
                    Err(err.into())
                }
            }
        }
    }
    /// Lists all tables in the given namespace.
    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error> {
        let tables = catalog_api_api::list_tables(
            &self.configuration,
            self.name.as_deref(),
            &namespace.to_string(),
            None,
            None,
        )
        .await
        .map_err(Into::<Error>::into)?;
        let tables = tables.identifiers.unwrap_or(Vec::new()).into_iter();
        let views = catalog_api_api::list_views(
            &self.configuration,
            self.name.as_deref(),
            &namespace.to_string(),
            None,
            None,
        )
        .await
        .map_err(Into::<Error>::into)?;
        Ok(views
            .identifiers
            .unwrap_or(Vec::new())
            .into_iter()
            .chain(tables)
            .collect())
    }
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, Error> {
        let namespaces = catalog_api_api::list_namespaces(
            &self.configuration,
            self.name.as_deref(),
            None,
            None,
            parent,
        )
        .await
        .map_err(Into::<Error>::into)?;
        namespaces
            .namespaces
            .ok_or(Error::NotFound(format!(
                "Namespaces in catalog {:?}",
                self.name
            )))?
            .into_iter()
            .map(|x| Namespace::try_new(&x))
            .collect::<Result<Vec<_>, iceberg_rust::spec::error::Error>>()
            .map_err(Error::from)
    }
    /// Check if a table exists
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, Error> {
        catalog_api_api::view_exists(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .or_else(|_| async move {
            catalog_api_api::table_exists(
                &self.configuration,
                self.name.as_deref(),
                &identifier.namespace().to_string(),
                identifier.name(),
            )
            .await
        })
        .await
        .map(|_| true)
        .map_err(Into::<Error>::into)
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error> {
        catalog_api_api::drop_table(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
            None,
        )
        .await
        .map_err(Into::<Error>::into)
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), Error> {
        catalog_api_api::drop_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .await
        .map_err(Into::<Error>::into)
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), Error> {
        catalog_api_api::drop_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .await
        .map_err(Into::<Error>::into)
    }
    /// Load a table.
    async fn load_tabular(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, Error> {
        // Load View/Matview metadata, is loaded as tabular to enable both possibilities. Must not be table metadata
        let tabular_metadata = catalog_api_api::load_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
        )
        .await
        .map(|x| x.metadata);
        match tabular_metadata {
            Ok(TabularMetadata::View(view)) => Ok(Tabular::View(
                View::new(identifier.clone(), self.clone(), view).await?,
            )),
            Ok(TabularMetadata::MaterializedView(matview)) => Ok(Tabular::MaterializedView(
                MaterializedView::new(identifier.clone(), self.clone(), matview).await?,
            )),
            Err(apis::Error::ResponseError(content)) => {
                if content.status == 404 {
                    let table_metadata = catalog_api_api::load_table(
                        &self.configuration,
                        self.name.as_deref(),
                        &identifier.namespace().to_string(),
                        identifier.name(),
                        None,
                        None,
                    )
                    .await
                    .map(|x| x.metadata)
                    .map_err(|_| Error::CatalogNotFound)?;

                    Ok(Tabular::Table(
                        Table::new(identifier.clone(), self.clone(), table_metadata).await?,
                    ))
                } else {
                    Err(Into::<Error>::into(apis::Error::ResponseError(content)))
                }
            }
            _ => Err(Error::InvalidFormat(
                "Entity returned from load_view cannot be a table.".to_owned(),
            )),
        }
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        create_table: CreateTable,
    ) -> Result<Table, Error> {
        catalog_api_api::create_table(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            create_table,
            None,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            async move { Table::new(identifier.clone(), clone, response.metadata).await }
        })
        .await
    }
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        commit: iceberg_rust::catalog::commit::CommitTable,
    ) -> Result<Table, Error> {
        let identifier = commit.identifier.clone();
        catalog_api_api::update_table(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
            commit,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            let identifier = identifier.clone();
            async move { Table::new(identifier, clone, response.metadata).await }
        })
        .await
    }
    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, Error> {
        catalog_api_api::create_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            create_view,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            async move {
                if let TabularMetadata::View(metadata) = response.metadata {
                    View::new(identifier.clone(), clone, metadata).await
                } else {
                    Err(Error::InvalidFormat(
                        "Create view didn't return view metadata.".to_owned(),
                    ))
                }
            }
        })
        .await
    }
    async fn update_view(self: Arc<Self>, commit: CommitView<Option<()>>) -> Result<View, Error> {
        let identifier = commit.identifier.clone();
        catalog_api_api::replace_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
            commit,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            let identifier = identifier.clone();
            async move {
                if let TabularMetadata::View(metadata) = response.metadata {
                    View::new(identifier.clone(), clone, metadata).await
                } else {
                    Err(Error::InvalidFormat(
                        "Create view didn't return view metadata.".to_owned(),
                    ))
                }
            }
        })
        .await
    }
    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, Error> {
        let (create_view, mut create_table) = create_view.into();
        create_table.name.clone_from(&create_view.name);
        catalog_api_api::create_table(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            create_table,
            None,
        )
        .map_err(Into::<Error>::into)
        .await?;
        catalog_api_api::create_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            create_view,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            async move {
                if let TabularMetadata::MaterializedView(metadata) = response.metadata {
                    MaterializedView::new(identifier.clone(), clone, metadata).await
                } else {
                    Err(Error::InvalidFormat(
                        "Create materialzied view didn't return materialized view metadata."
                            .to_owned(),
                    ))
                }
            }
        })
        .await
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, Error> {
        let identifier = commit.identifier.clone();
        catalog_api_api::replace_view(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            identifier.name(),
            commit,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            let identifier = identifier.clone();
            async move {
                if let TabularMetadata::MaterializedView(metadata) = response.metadata {
                    MaterializedView::new(identifier.clone(), clone, metadata).await
                } else {
                    Err(Error::InvalidFormat(
                        "Create materialzied view didn't return materialized view metadata."
                            .to_owned(),
                    ))
                }
            }
        })
        .await
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_location: &str,
    ) -> Result<Table, Error> {
        let request = models::RegisterTableRequest::new(
            identifier.name().to_owned(),
            metadata_location.to_owned(),
        );

        catalog_api_api::register_table(
            &self.configuration,
            self.name.as_deref(),
            &identifier.namespace().to_string(),
            request,
        )
        .map_err(Into::<Error>::into)
        .and_then(|response| {
            let clone = self.clone();
            async move { Table::new(identifier.clone(), clone, response.metadata).await }
        })
        .await
    }
    /// Return an object store for the desired bucket
    fn object_store(&self, bucket: Bucket) -> Arc<dyn ObjectStore> {
        self.object_store_builder.build(bucket).unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct RestCatalogList {
    configuration: Configuration,
    object_store_builder: ObjectStoreBuilder,
}

impl RestCatalogList {
    pub fn new(configuration: Configuration, object_store_builder: ObjectStoreBuilder) -> Self {
        Self {
            configuration,
            object_store_builder,
        }
    }
}

#[async_trait]
impl CatalogList for RestCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(RestCatalog::new(
            Some(name),
            self.configuration.clone(),
            self.object_store_builder.clone(),
        )))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(test)]
pub mod tests {
    use datafusion::{
        arrow::array::{Float64Array, Int64Array},
        common::tree_node::{TransformedResult, TreeNode},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_iceberg::{
        catalog::catalog::IcebergCatalog,
        planner::{iceberg_transform, IcebergQueryPlanner},
    };
    use iceberg_rust::{
        catalog::{identifier::Identifier, namespace::Namespace, Catalog},
        object_store::ObjectStoreBuilder,
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::Table,
    };
    use object_store::{memory::InMemory, ObjectStore};
    use std::{convert::TryFrom, sync::Arc, time::Duration};
    use testcontainers::{
        core::{wait::LogWaitStrategy, ExecCommand, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt,
    };
    use testcontainers_modules::localstack::LocalStack;
    use tokio::time::sleep;

    use crate::{apis::configuration::Configuration, catalog::RestCatalog};

    fn configuration(url: &str) -> Configuration {
        Configuration {
            base_path: url.to_owned(),
            user_agent: None,
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
            aws_v4_key: None,
        }
    }
    #[tokio::test]
    async fn test_create_update_drop_table() {
        let docker_host = "172.17.0.1";

        let localstack = LocalStack::default()
            .with_env_var("SERVICES", "s3")
            .with_env_var("AWS_ACCESS_KEY_ID", "user")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
            .start()
            .await
            .unwrap();

        localstack
            .exec(ExecCommand::new(vec![
                "awslocal",
                "s3api",
                "create-bucket",
                "--bucket",
                "warehouse",
            ]))
            .await
            .unwrap();

        let localstack_host = localstack.get_host().await.unwrap();
        let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

        let rest = GenericImage::new("apache/iceberg-rest-fixture", "latest")
            .with_wait_for(WaitFor::Log(LogWaitStrategy::stderr(
                "INFO org.eclipse.jetty.server.Server - Started ",
            )))
            .with_env_var("AWS_REGION", "us-east-1")
            .with_env_var("AWS_ACCESS_KEY_ID", "user")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
            .with_env_var("CATALOG_WAREHOUSE", "s3://warehouse/")
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var(
                "CATALOG_S3_ENDPOINT",
                format!("http://{}:{}", &docker_host, &localstack_port),
            )
            .start()
            .await
            .unwrap();

        let rest_host = rest.get_host().await.unwrap();
        let rest_port = rest.get_host_port_ipv4(8181).await.unwrap();

        let object_store = ObjectStoreBuilder::s3()
            .with_config("aws_access_key_id".parse().unwrap(), "user")
            .with_config("aws_secret_access_key".parse().unwrap(), "password")
            .with_config(
                "endpoint".parse().unwrap(),
                format!("http://{}:{}", localstack_host, localstack_port),
            )
            .with_config("region".parse().unwrap(), "us-east-1")
            .with_config("allow_http".parse().unwrap(), "true");

        let iceberg_catalog = Arc::new(RestCatalog::new(
            None,
            configuration(&format!("http://{rest_host}:{rest_port}")),
            object_store,
        ));

        iceberg_catalog
            .create_namespace(&Namespace::try_new(&["tpch".to_owned()]).unwrap(), None)
            .await
            .expect("Failed to create namespace");

        let catalog = Arc::new(
            IcebergCatalog::new(iceberg_catalog.clone(), None)
                .await
                .unwrap(),
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_catalog("warehouse", catalog);

        let sql = "CREATE EXTERNAL TABLE lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../../datafusion_iceberg/testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE warehouse.tpch.lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION 's3://warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let tables = iceberg_catalog
            .clone()
            .list_tabulars(
                &Namespace::try_new(&["tpch".to_owned()]).expect("Failed to create namespace"),
            )
            .await
            .expect("Failed to list Tables");
        assert_eq!(tables[0].to_string(), "tpch.lineitem".to_owned());

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let batches = ctx
        .sql("select sum(L_QUANTITY), L_PARTKEY from warehouse.tpch.lineitem group by L_PARTKEY;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, product_ids) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 24027 {
                        assert_eq!(amount.unwrap(), 24.0)
                    } else if product_id.unwrap() == 63700 {
                        assert_eq!(amount.unwrap(), 23.0)
                    }
                }
                once = true
            }
        }

        assert!(once);
    }
}
