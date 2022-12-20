use async_trait::async_trait;
use log::info;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, ModelTrait, QueryFilter,
    TransactionTrait,
};
use serde_json::json;
use swagger::{Has, XSpanIdString};

use iceberg_catalog_rest_rdbms_server::models::{self, TableMetadata};

use iceberg_catalog_rest_rdbms_server::{
    Api, CreateNamespaceResponse, CreateTableResponse, DropNamespaceResponse, DropTableResponse,
    GetConfigResponse, GetTokenResponse, ListNamespacesResponse, ListTablesResponse,
    LoadNamespaceMetadataResponse, LoadTableResponse, RenameTableResponse, ReportMetricsResponse,
    TableExistsResponse, UpdatePropertiesResponse, UpdateTableResponse,
};

use swagger::ApiError;

use crate::database::entities::{prelude::*, *};

#[derive(Clone)]
pub struct Server {
    db: DatabaseConnection,
}

impl Server {
    pub fn new(db: DatabaseConnection) -> Self {
        Server { db }
    }
}

#[async_trait]
impl<C> Api<C> for Server
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        _context: &C,
    ) -> Result<CreateNamespaceResponse, ApiError> {
        let name = iter_tools::intersperse(
            create_namespace_request
                .ok_or(ApiError("Missing CreateNamespaceRequest.".into()))?
                .namespace
                .into_iter(),
            ".".to_owned(),
        )
        .collect::<String>();
        self.db
            .transaction::<_, CreateNamespaceResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        match Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                        {
                            None => {
                                let new_catalog = catalog::ActiveModel::from_json(json!({
                                    "name": &prefix,
                                }))?;

                                let result = Catalog::insert(new_catalog.clone())
                                    .on_conflict(
                                        // on conflict update
                                        OnConflict::column(catalog::Column::Name)
                                            .do_nothing()
                                            .to_owned(),
                                    )
                                    .exec(txn)
                                    .await?;

                                Catalog::find_by_id(result.last_insert_id).one(txn).await?
                            }
                            x => x,
                        }
                    } else {
                        None
                    };

                    let new_namespace = namespace::ActiveModel::from_json(json!({
                        "name": &name,
                        "catalog_id": catalog.as_ref().map(|catalog| catalog.id)
                    }))?;

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&name))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&name))
                                .one(txn)
                                .await?
                        }
                    };

                    match namespace {
                        None => {
                            Namespace::insert(new_namespace)
                            .on_conflict(
                                // on conflict update
                                OnConflict::column(namespace::Column::Name)
                                    .do_nothing()
                                    .to_owned(),
                            )
                            .exec(txn)
                            .await?;
                            
                            Ok(
                            CreateNamespaceResponse::RepresentsASuccessfulCallToCreateANamespace(
                                models::CreateNamespace200Response {
                                    namespace: vec![name],
                                    properties: None,
                                },
                            ),
                        )},
                        Some(_) => {
                            Ok(CreateNamespaceResponse::Conflict(models::ErrorModel::new(
                                "The namespace already exists".into(),
                                "Conflict".into(),
                                500,
                            )))
                        }
                    }

                    
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        _context: &C,
    ) -> Result<CreateTableResponse, ApiError> {
        let create_table_request =
            create_table_request.ok_or(ApiError("Missing CreateNamespaceRequest.".into()))?;
        let name = create_table_request.name;
        let metadata_location = create_table_request
            .location
            .ok_or(ApiError("Missing metadata_location.".into()))?;
        self.db
            .transaction::<_, CreateTableResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        match Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                        {
                            None => {
                                let new_catalog = catalog::ActiveModel::from_json(json!({
                                    "name": &prefix,
                                }))?;

                                let result = Catalog::insert(new_catalog.clone())
                                    .on_conflict(
                                        // on conflict update
                                        OnConflict::column(catalog::Column::Name)
                                            .do_nothing()
                                            .to_owned(),
                                    )
                                    .exec(txn)
                                    .await?;

                                Catalog::find_by_id(result.last_insert_id).one(txn).await?
                            }
                            x => x,
                        }
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    // Check if namespace exists
                    match namespace {
                        None => Ok(CreateTableResponse::NotFound(models::ErrorModel::new(
                            "The namespace specified does not exist".into(),
                            "NotFound".into(),
                            500,
                        ))),
                        Some(namespace) => {
                            let table = namespace
                            .find_related(IcebergTable)
                            .filter(iceberg_table::Column::Name.contains(&name))
                            .one(txn)
                            .await?;
                            match table {
                                Some(_) => Ok(CreateTableResponse::Conflict(models::ErrorModel::new(
                                    "The table already exists.".into(),
                                    "Conflict".into(),
                                    500,
                                ))),
                                None => {
                                    let new_table = iceberg_table::ActiveModel::from_json(json!({
                                        "name": &name,
                                        "namespace_id": namespace.id,
                                        "metadata_location": metadata_location,
                                        "previous_metadata_location": None::<String>
                                    }))?;
        
                                    IcebergTable::insert(new_table)
                                        .on_conflict(
                                            // on conflict update
                                            OnConflict::column(iceberg_table::Column::Name)
                                                .do_nothing()
                                                .to_owned(),
                                        )
                                        .exec(txn)
                                        .await?;
        
                                    Ok(CreateTableResponse::TableMetadataResultAfterCreatingATable(
                                        models::LoadTableResult {
                                            metadata_location: Some(metadata_location.to_string()),
                                            config: None,
                                            metadata: TableMetadata::new(2, "".into()),
                                        },
                                    ))
                                }
                            }
                        }
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        _context: &C,
    ) -> Result<DropNamespaceResponse, ApiError> {
        self.db
            .transaction::<_, DropNamespaceResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    match namespace {
                        None => Ok(DropNamespaceResponse::NotFound(models::ErrorModel::new(
                            "Namespace to delete does not exist.".into(),
                            "NotFound".into(),
                            500,
                        ))),
                        Some(namespace) => {
                            namespace.delete(txn).await?;

                            Ok(DropNamespaceResponse::Success)
                        }
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        _purge_requested: Option<bool>,
        _context: &C,
    ) -> Result<DropTableResponse, ApiError> {
        self.db
            .transaction::<_, DropTableResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    let table = match namespace {
                        None => {
                            IcebergTable::find()
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                        Some(namespace) => {
                            namespace
                                .find_related(IcebergTable)
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                    };
                    match table {
                        None => Ok(DropTableResponse::NotFound(models::ErrorModel::new(
                            "Namespace to delete does not exist.".into(),
                            "NotFound".into(),
                            500,
                        ))),
                        Some(table) => {
                            table.delete(txn).await?;

                            Ok(DropTableResponse::Success)
                        }
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        _parent: Option<String>,
        _context: &C,
    ) -> Result<ListNamespacesResponse, ApiError> {
        self.db
            .transaction::<_, ListNamespacesResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespaces = match catalog {
                        None => Namespace::find().all(txn).await?,
                        Some(catalog) => catalog.find_related(Namespace).all(txn).await?,
                    };

                    Ok(ListNamespacesResponse::AListOfNamespaces(
                        models::ListNamespaces200Response {
                            namespaces: Some(namespaces
                                .into_iter()
                                .map(|x| x.name.split(".").map(|x|x.to_owned()).collect())
                                .collect()),
                        },
                    ))
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        _context: &C,
    ) -> Result<ListTablesResponse, ApiError> {
        self.db
            .transaction::<_, ListTablesResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    let tables = match namespace.as_ref() {
                        None => IcebergTable::find().all(txn).await?,
                        Some(namespace) => namespace.find_related(IcebergTable).all(txn).await?,
                    };
                    Ok(ListTablesResponse::AListOfTableIdentifiers(
                        models::ListTables200Response {
                            identifiers: Some(
                                tables
                                    .into_iter()
                                    .map(|x| models::TableIdentifier {
                                        name: x.name,
                                        namespace: match namespace.as_ref() {
                                            Some(namespace) => namespace
                                                .name
                                                .split('.')
                                                .map(|s| s.to_owned())
                                                .collect(),
                                            None => vec![],
                                        },
                                    })
                                    .collect(),
                            ),
                        },
                    ))
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Load the metadata properties for a namespace
    async fn load_namespace_metadata(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<LoadNamespaceMetadataResponse, ApiError> {
        let context = context.clone();
        info!(
            "load_namespace_metadata(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Load a table from the catalog
    async fn load_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        _context: &C,
    ) -> Result<LoadTableResponse, ApiError> {
        self.db
            .transaction::<_, LoadTableResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    let table = match namespace {
                        None => {
                            IcebergTable::find()
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                        Some(namespace) => {
                            namespace
                                .find_related(IcebergTable)
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                    };
                    match table {
                        None => Ok(LoadTableResponse::NotFound(models::ErrorModel::new(
                            "Namespace to delete does not exist.".into(),
                            "NotFound".into(),
                            500,
                        ))),
                        Some(table) => Ok(LoadTableResponse::TableMetadataResultWhenLoadingATable(
                            models::LoadTableResult {
                                metadata_location: Some(table.metadata_location.to_string()),
                                config: None,
                                metadata: TableMetadata::new(2, "".into()),
                            },
                        )),
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        _context: &C,
    ) -> Result<RenameTableResponse, ApiError> {
        self.db
            .transaction::<_, RenameTableResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let old_namespace_name = iter_tools::intersperse(
                        rename_table_request.source.namespace.into_iter(),
                        ".".into(),
                    )
                    .collect::<String>();
                    let old_name = rename_table_request.source.name;
                    let new_namespace_name = iter_tools::intersperse(
                        rename_table_request.destination.namespace.into_iter(),
                        ".".into(),
                    )
                    .collect::<String>();
                    let new_name = rename_table_request.destination.name;
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let old_namespace = match &catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&old_namespace_name))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&old_namespace_name))
                                .one(txn)
                                .await?
                        }
                    };

                    let new_namespace = if old_namespace_name == new_namespace_name {
                        old_namespace.clone()
                    } else {
                        match &catalog {
                            None => {
                                Namespace::find()
                                    .filter(namespace::Column::Name.contains(&new_namespace_name))
                                    .one(txn)
                                    .await?
                            }
                            Some(catalog) => {
                                catalog
                                    .find_related(Namespace)
                                    .filter(namespace::Column::Name.contains(&new_namespace_name))
                                    .one(txn)
                                    .await?
                            }
                        }
                    };

                    let table = match old_namespace {
                        None => {
                            IcebergTable::find()
                                .filter(iceberg_table::Column::Name.contains(&old_name))
                                .one(txn)
                                .await?
                        }
                        Some(old_namespace) => {
                            old_namespace
                                .find_related(IcebergTable)
                                .filter(iceberg_table::Column::Name.contains(&old_name))
                                .one(txn)
                                .await?
                        }
                    };
                    match (table, new_namespace) {
                        (None, _) => Ok(RenameTableResponse::NotFound(models::ErrorModel::new(
                            "Table to rename does not exist.".into(),
                            "Not Found - NoSuchTableException".into(),
                            500,
                        ))),
                        (_, None) => Ok(RenameTableResponse::NotFound(models::ErrorModel::new(
                            "The target namespace of the new table identifier does not exist."
                                .into(),
                            "NoSuchNamespaceException".into(),
                            500,
                        ))),
                        (Some(table), Some(namespace)) => {
                            let mut new_table: iceberg_table::ActiveModel = table.into();
                            new_table.set(iceberg_table::Column::Name, new_name.into());
                            new_table.set(iceberg_table::Column::NamespaceId, namespace.id.into());
                            new_table.update(txn).await?;
                            Ok(RenameTableResponse::OK)
                        }
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        report_metrics_request: models::ReportMetricsRequest,
        context: &C,
    ) -> Result<ReportMetricsResponse, ApiError> {
        let context = context.clone();
        info!(
            "report_metrics(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            report_metrics_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Check if a table exists
    async fn table_exists(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        _context: &C,
    ) -> Result<TableExistsResponse, ApiError> {
        self.db
            .transaction::<_, TableExistsResponse, DbErr>(|txn| {
                Box::pin(async move {
                    let catalog = if prefix != "" {
                        Catalog::find()
                            .filter(catalog::Column::Name.contains(&prefix))
                            .one(txn)
                            .await?
                    } else {
                        None
                    };

                    let namespace = match catalog {
                        None => {
                            Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                        Some(catalog) => {
                            catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await?
                        }
                    };

                    let table = match namespace {
                        None => {
                            IcebergTable::find()
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                        Some(namespace) => {
                            namespace
                                .find_related(IcebergTable)
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await?
                        }
                    };
                    match table {
                        None => Ok(TableExistsResponse::NotFound),
                        Some(_) => Ok(TableExistsResponse::OK),
                    }
                })
            })
            .await
            .map_err(|err| ApiError(err.to_string()))
    }

    /// Set or remove properties on a namespace
    async fn update_properties(
        &self,
        prefix: String,
        namespace: String,
        update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest>,
        context: &C,
    ) -> Result<UpdatePropertiesResponse, ApiError> {
        let context = context.clone();
        info!(
            "update_properties(\"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            update_namespace_properties_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Commit updates to a table
    async fn update_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        commit_table_request: Option<models::CommitTableRequest>,
        _context: &C,
    ) -> Result<UpdateTableResponse, ApiError> {
        self.db.transaction::<_, UpdateTableResponse, DbErr>(|txn| {
            Box::pin(async move {
                match commit_table_request {
                    None => Ok(UpdateTableResponse::IndicatesABadRequestError(
                        models::ErrorModel::new("No CommitTableRequest".into(), "BadRequest".into(), 500),
                    )),
                    Some(commit_table_request) => {
                        let catalog = if prefix != "" {
                            Catalog::find()
                                .filter(catalog::Column::Name.contains(&prefix))
                                .one(txn)
                                .await
                                ?
                        } else {
                            None
                        };
        
                        let namespace = match catalog {
                            None => Namespace::find()
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await
                                ?,
                            Some(catalog) => catalog
                                .find_related(Namespace)
                                .filter(namespace::Column::Name.contains(&namespace))
                                .one(txn)
                                .await
                                ?,
                        };
        
                        let table = match namespace {
                            None => IcebergTable::find()
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await
                                ?,
                            Some(namespace) => namespace
                                .find_related(IcebergTable)
                                .filter(iceberg_table::Column::Name.contains(&table))
                                .one(txn)
                                .await
                                ?,
                        };
                        match table {
                            None => Ok(UpdateTableResponse::NotFound(models::ErrorModel::new(
                                "Namespace to delete does not exist.".into(),
                                "NotFound".into(),
                                500,
                            ))),
                            Some(table) => {
                                let old_metadata_location = table.metadata_location.clone();
                                let mut new_table: iceberg_table::ActiveModel = table.into();
                                new_table.set(
                                    iceberg_table::Column::MetadataLocation,
                                    commit_table_request
                                        .updates
                                        .iter()
                                        .last()
                                        .unwrap()
                                        .location
                                        .as_str()
                                        .into(),
                                );
                                new_table.set(
                                    iceberg_table::Column::PreviousMetadataLocation,
                                    Some(old_metadata_location).into(),
                                );
                                let new_table = new_table.update(txn).await?;
                                Ok(
                                    UpdateTableResponse::ResponseUsedWhenATableIsSuccessfullyUpdated(
                                        models::UpdateTable200Response {
                                            metadata_location: new_table.metadata_location,
                                            metadata: TableMetadata::new(2, "".into()),
                                        },
                                    ),
                                )
                            }
                        }
                    }
                }
            })
        })
        .await.map_err(|err| ApiError(err.to_string()))
        
    }

    /// List all catalog configuration settings
    async fn get_config(&self, _context: &C) -> Result<GetConfigResponse, ApiError> {
        Ok(GetConfigResponse::ServerSpecifiedConfigurationValues(
            models::CatalogConfig {
                overrides: serde_json::from_str("{}").map_err(|err| ApiError(err.to_string()))?,
                defaults: serde_json::from_str("{}").map_err(|err| ApiError(err.to_string()))?,
            },
        ))
    }

    /// Get a token using an OAuth2 flow
    async fn get_token(
        &self,
        grant_type: Option<String>,
        scope: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        requested_token_type: Option<models::TokenType>,
        subject_token: Option<String>,
        subject_token_type: Option<models::TokenType>,
        actor_token: Option<String>,
        actor_token_type: Option<models::TokenType>,
        context: &C,
    ) -> Result<GetTokenResponse, ApiError> {
        let context = context.clone();
        info!(
            "get_token({:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}) - X-Span-ID: {:?}",
            grant_type,
            scope,
            client_id,
            client_secret,
            requested_token_type,
            subject_token,
            subject_token_type,
            actor_token,
            actor_token_type,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }
}

#[cfg(test)]

pub mod tests {
    use std::collections::HashMap;

    use iceberg_catalog_rest_client::{
        apis::{self, configuration::Configuration},
        models::{self, schema, Schema},
    };

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
    async fn create_namespace() {
        let request = models::CreateNamespaceRequest {
            namespace: vec!["create_namespace".to_owned()],
            properties: None,
        };
        let response =
            apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request))
                .await
                .expect("Failed to create namespace");
        assert_eq!(response.namespace[0], "create_namespace");

        apis::catalog_api_api::drop_namespace(&configuration(), "my_catalog", "create_namespace")
            .await
            .expect("Failed to drop namespace");
    }

    #[tokio::test]
    async fn drop_namespace() {
        let request = models::CreateNamespaceRequest {
            namespace: vec!["drop_namespace".to_owned()],
            properties: None,
        };
        apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request))
            .await
            .expect("Failed to create namespace");

        apis::catalog_api_api::drop_namespace(&configuration(), "my_catalog", "drop_namespace")
            .await
            .expect("Failed to drop namespace");
    }

    #[tokio::test]
    async fn create_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["create_table".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut request = models::CreateTableRequest::new(
            "create_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        let response = apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "create_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");
        assert_eq!(response.metadata_location.unwrap(), "s3://path/to/location");

        apis::catalog_api_api::drop_table(
            &configuration(),
            "my_catalog",
            "create_table",
            "create_table",
            Some(true),
        )
        .await
        .expect("Failed to create namespace");
    }

    #[tokio::test]
    async fn drop_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["drop_table".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut request = models::CreateTableRequest::new(
            "drop_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "drop_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        apis::catalog_api_api::drop_table(
            &configuration(),
            "my_catalog",
            "drop_table",
            "drop_table",
            Some(true),
        )
        .await
        .expect("Failed to create namespace");
    }

    #[tokio::test]
    async fn list_namespaces() {
        let request1 = models::CreateNamespaceRequest {
            namespace: vec!["list_namespaces1".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request1))
            .await;

        let request2 = models::CreateNamespaceRequest {
            namespace: vec!["list_namespaces2".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(&configuration(), "my_catalog", Some(request2))
            .await;

        let response = apis::catalog_api_api::list_namespaces(&configuration(), "my_catalog", None)
            .await
            .expect("Failed to list namespace");
        assert!(
            response.namespaces.as_ref().unwrap().iter().any(|x| x[0] == "list_namespaces1")
        );
        assert!(
            response.namespaces.as_ref().unwrap().iter().any(|x| x[0] == "list_namespaces2")
        );
    }

    #[tokio::test]
    async fn list_tables() {
        let namespace_request1 = models::CreateNamespaceRequest {
            namespace: vec!["list_tables".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request1),
        )
        .await;

        let mut request1 = models::CreateTableRequest::new(
            "list_tables1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request1.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request1),
        )
        .await
        .expect("Failed to create table");
        let mut request2 = models::CreateTableRequest::new(
            "list_tables2".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request2.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "list_tables",
            Some(request2),
        )
        .await
        .expect("Failed to create table");
        let response =
            apis::catalog_api_api::list_tables(&configuration(), "my_catalog", "list_tables")
                .await
                .expect("Failed to create table");
        assert_eq!(
            response
                .identifiers
                .unwrap()
                .into_iter()
                .map(|x| x.name)
                .collect::<Vec<_>>(),
            vec!["list_tables1", "list_tables2"]
        );
    }

    #[tokio::test]
    async fn load_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["load_table".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut request = models::CreateTableRequest::new(
            "load_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        request.location = Some("s3://path/to/location".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "load_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        let response = apis::catalog_api_api::load_table(
            &configuration(),
            "my_catalog",
            "load_table",
            "load_table",
        )
        .await
        .expect("Failed to create namespace");

        assert_eq!(response.metadata_location.unwrap(), "s3://path/to/location")
    }

    #[tokio::test]
    async fn update_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["update_table".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut create_request = models::CreateTableRequest::new(
            "update_table".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        let create_response = apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "update_table",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        let request = models::CommitTableRequest::new(
            vec![],
            vec![models::TableUpdate::new(
                models::table_update::Action::SetLocation,
                create_response.metadata.format_version,
                create_response
                    .metadata
                    .schemas
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::Schema::new(schema::RHashType::default(), vec![])),
                create_response.metadata.current_schema_id.unwrap_or(0),
                create_response
                    .metadata
                    .partition_specs
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::PartitionSpec::default()),
                create_response.metadata.default_spec_id.unwrap_or(0),
                create_response
                    .metadata
                    .sort_orders
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::SortOrder::default()),
                create_response.metadata.default_sort_order_id.unwrap_or(0),
                create_response
                    .metadata
                    .snapshots
                    .map(|x| x[0].clone())
                    .unwrap_or_else(|| models::Snapshot::default()),
                models::table_update::RHashType::default(),
                create_response.metadata.current_snapshot_id.unwrap_or(0) as i64,
                "update_table".into(),
                vec![],
                "s3://path/to/location2".into(),
                HashMap::new(),
                vec![],
            )],
        );
        let response = apis::catalog_api_api::update_table(
            &configuration(),
            "my_catalog",
            "update_table",
            "update_table",
            Some(request),
        )
        .await
        .expect("Failed to create table");

        assert_eq!(response.metadata_location, "s3://path/to/location2");

        apis::catalog_api_api::drop_table(
            &configuration(),
            "my_catalog",
            "update_table",
            "update_table",
            Some(true),
        )
        .await
        .expect("Failed to delete table");
    }

    #[tokio::test]
    async fn rename_table() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["rename_table".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut create_request = models::CreateTableRequest::new(
            "rename_table1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "rename_table",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        let request = models::RenameTableRequest::new(
            models::TableIdentifier {
                name: "rename_table1".into(),
                namespace: vec!["rename_table".into()],
            },
            models::TableIdentifier {
                name: "rename_table2".into(),
                namespace: vec!["rename_table".into()],
            },
        );
        apis::catalog_api_api::rename_table(&configuration(), "my_catalog", request)
            .await
            .expect("Failed to create table");

        apis::catalog_api_api::table_exists(
            &configuration(),
            "my_catalog",
            "rename_table",
            "rename_table2",
        )
        .await
        .expect("Failed to create table");

        apis::catalog_api_api::drop_table(
            &configuration(),
            "my_catalog",
            "rename_table",
            "rename_table2",
            Some(true),
        )
        .await
        .expect("Failed to delete table");
    }

    #[tokio::test]
    async fn table_exists() {
        let namespace_request = models::CreateNamespaceRequest {
            namespace: vec!["table_exists".to_owned()],
            properties: None,
        };
        let _ = apis::catalog_api_api::create_namespace(
            &configuration(),
            "my_catalog",
            Some(namespace_request),
        )
        .await;

        let mut create_request = models::CreateTableRequest::new(
            "table_exists1".to_owned(),
            Schema::new(schema::RHashType::default(), vec![]),
        );
        create_request.location = Some("s3://path/to/location1".into());
        apis::catalog_api_api::create_table(
            &configuration(),
            "my_catalog",
            "table_exists",
            Some(create_request),
        )
        .await
        .expect("Failed to create table");

        apis::catalog_api_api::table_exists(
            &configuration(),
            "my_catalog",
            "table_exists",
            "table_exists1",
        )
        .await
        .expect("Failed to create table");
    }
}
