//! Main library entry point for openapi_client implementation.

#![allow(unused_imports)]

use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::server::conn::Http;
use hyper::service::Service;
use log::info;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use swagger::auth::MakeAllowAllAuthenticator;
use swagger::EmptyContext;
use swagger::{Has, XSpanIdString};
use tokio::net::TcpListener;

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
use openssl::ssl::{Ssl, SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};

use iceberg_catalog_rest_rdbms_server::models;

/// Builds an SSL implementation for Simple HTTPS from some hard-coded file names
pub async fn create(addr: &str, https: bool) {
    let addr = addr.parse().expect("Failed to parse bind address");

    let server = Server::new();

    let service = MakeService::new(server);

    let service = MakeAllowAllAuthenticator::new(service, "cosmo");

    #[allow(unused_mut)]
    let mut service = iceberg_catalog_rest_rdbms_server::server::context::MakeAddContext::<
        _,
        EmptyContext,
    >::new(service);

    if https {
        #[cfg(any(target_os = "macos", target_os = "windows", target_os = "ios"))]
        {
            unimplemented!("SSL is not implemented for the examples on MacOS, Windows or iOS");
        }

        #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
        {
            let mut ssl = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())
                .expect("Failed to create SSL Acceptor");

            // Server authentication
            ssl.set_private_key_file("examples/server-key.pem", SslFiletype::PEM)
                .expect("Failed to set private key");
            ssl.set_certificate_chain_file("examples/server-chain.pem")
                .expect("Failed to set certificate chain");
            ssl.check_private_key()
                .expect("Failed to check private key");

            let tls_acceptor = ssl.build();
            let tcp_listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                if let Ok((tcp, _)) = tcp_listener.accept().await {
                    let ssl = Ssl::new(tls_acceptor.context()).unwrap();
                    let addr = tcp.peer_addr().expect("Unable to get remote address");
                    let service = service.call(addr);

                    tokio::spawn(async move {
                        let tls = tokio_openssl::SslStream::new(ssl, tcp).map_err(|_| ())?;
                        let service = service.await.map_err(|_| ())?;

                        Http::new()
                            .serve_connection(tls, service)
                            .await
                            .map_err(|_| ())
                    });
                }
            }
        }
    } else {
        // Using HTTP
        hyper::server::Server::bind(&addr)
            .serve(service)
            .await
            .unwrap()
    }
}

#[derive(Copy, Clone)]
pub struct Server<C> {
    marker: PhantomData<C>,
}

impl<C> Server<C> {
    pub fn new() -> Self {
        Server {
            marker: PhantomData,
        }
    }
}

use iceberg_catalog_rest_rdbms_server::server::MakeService;
use iceberg_catalog_rest_rdbms_server::{
    Api, CreateNamespaceResponse, CreateTableResponse, DropNamespaceResponse, DropTableResponse,
    GetConfigResponse, GetTokenResponse, ListNamespacesResponse, ListTablesResponse,
    LoadNamespaceMetadataResponse, LoadTableResponse, RenameTableResponse, ReportMetricsResponse,
    TableExistsResponse, UpdatePropertiesResponse, UpdateTableResponse,
};
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        context: &C,
    ) -> Result<CreateNamespaceResponse, ApiError> {
        let context = context.clone();
        info!(
            "create_namespace(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            create_namespace_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        context: &C,
    ) -> Result<CreateTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "create_table(\"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            create_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<DropNamespaceResponse, ApiError> {
        let context = context.clone();
        info!(
            "drop_namespace(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        purge_requested: Option<bool>,
        context: &C,
    ) -> Result<DropTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "drop_table(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            purge_requested,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        parent: Option<String>,
        context: &C,
    ) -> Result<ListNamespacesResponse, ApiError> {
        let context = context.clone();
        info!(
            "list_namespaces(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            parent,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        context: &C,
    ) -> Result<ListTablesResponse, ApiError> {
        let context = context.clone();
        info!(
            "list_tables(\"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
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
        context: &C,
    ) -> Result<LoadTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "load_table(\"{}\", \"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        context: &C,
    ) -> Result<RenameTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "rename_table(\"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            rename_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
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
        context: &C,
    ) -> Result<TableExistsResponse, ApiError> {
        let context = context.clone();
        info!(
            "table_exists(\"{}\", \"{}\", \"{}\") - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
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
        context: &C,
    ) -> Result<UpdateTableResponse, ApiError> {
        let context = context.clone();
        info!(
            "update_table(\"{}\", \"{}\", \"{}\", {:?}) - X-Span-ID: {:?}",
            prefix,
            namespace,
            table,
            commit_table_request,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// List all catalog configuration settings
    async fn get_config(&self, context: &C) -> Result<GetConfigResponse, ApiError> {
        let context = context.clone();
        info!("get_config() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
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
