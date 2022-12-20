#![allow(missing_docs, trivial_casts, unused_variables, unused_mut, unused_imports, unused_extern_crates, non_camel_case_types)]
#![allow(unused_imports, unused_attributes)]
#![allow(clippy::derive_partial_eq_without_eq, clippy::blacklisted_name)]

use async_trait::async_trait;
use futures::Stream;
use std::error::Error;
use std::task::{Poll, Context};
use swagger::{ApiError, ContextWrapper};
use serde::{Serialize, Deserialize};

type ServiceError = Box<dyn Error + Send + Sync + 'static>;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "0.0.1";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum CreateNamespaceResponse {
    /// Represents a successful call to create a namespace. Returns the namespace created, as well as any properties that were stored for the namespace, including those the server might have added. Implementations are not required to support namespace properties.
    RepresentsASuccessfulCallToCreateANamespace
    (models::CreateNamespace200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Acceptable / Unsupported Operation. The server does not support this operation.
    NotAcceptable
    (models::ErrorModel)
    ,
    /// Conflict - The namespace already exists
    Conflict
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum CreateTableResponse {
    /// Table metadata result after creating a table
    TableMetadataResultAfterCreatingATable
    (models::LoadTableResult)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - The namespace specified does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Conflict - The table already exists
    Conflict
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DropNamespaceResponse {
    /// Success, no content
    Success
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - Namespace to delete does not exist.
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DropTableResponse {
    /// Success, no content
    Success
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - NoSuchTableException, Table to drop does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum ListNamespacesResponse {
    /// A list of namespaces
    AListOfNamespaces
    (models::ListNamespaces200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - Namespace provided in the `parent` query parameter is not found.
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum ListTablesResponse {
    /// A list of table identifiers
    AListOfTableIdentifiers
    (models::ListTables200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - The namespace specified does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum LoadNamespaceMetadataResponse {
    /// Returns a namespace, as well as any properties stored on the namespace if namespace properties are supported by the server.
    ReturnsANamespace
    (models::LoadNamespaceMetadata200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - Namespace not found
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum LoadTableResponse {
    /// Table metadata result when loading a table
    TableMetadataResultWhenLoadingATable
    (models::LoadTableResult)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - NoSuchTableException, table to load does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum RenameTableResponse {
    /// OK
    OK
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Not Acceptable / Unsupported Operation. The server does not support this operation.
    NotAcceptable
    (models::ErrorModel)
    ,
    /// Conflict - The target table identifier to rename to already exists
    Conflict
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum ReportMetricsResponse {
    /// Success, no content
    Success
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - NoSuchTableException, table to load does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum TableExistsResponse {
    /// OK - Table Exists
    OK
    ,
    /// Bad Request
    BadRequest
    ,
    /// Unauthorized
    Unauthorized
    ,
    /// Not Found
    NotFound
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum UpdatePropertiesResponse {
    /// JSON data response for a synchronous update properties request.
    JSONDataResponseForASynchronousUpdatePropertiesRequest
    (models::UpdateProperties200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - Namespace not found
    NotFound
    (models::ErrorModel)
    ,
    /// Not Acceptable / Unsupported Operation. The server does not support this operation.
    NotAcceptable
    (models::ErrorModel)
    ,
    /// Unprocessable Entity - A property key was included in both `removals` and `updates`
    UnprocessableEntity
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum UpdateTableResponse {
    /// Response used when a table is successfully updated. The table metadata JSON is returned in the metadata field. The corresponding file location of table metadata must be returned in the metadata-location field. Clients can check whether metadata has changed by comparing metadata locations.
    ResponseUsedWhenATableIsSuccessfullyUpdated
    (models::UpdateTable200Response)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Not Found - NoSuchTableException, table to load does not exist
    NotFound
    (models::ErrorModel)
    ,
    /// Conflict - CommitFailedException, one or more requirements failed. The client may retry.
    Conflict
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// An unknown server-side problem occurred; the commit state is unknown.
    AnUnknownServer
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side gateway timeout occurred; the commit state is unknown.
    AServer
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable on the client.
    AServer_2
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum GetConfigResponse {
    /// Server specified configuration values.
    ServerSpecifiedConfigurationValues
    (models::CatalogConfig)
    ,
    /// Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.
    IndicatesABadRequestError
    (models::ErrorModel)
    ,
    /// Unauthorized. Authentication is required and has failed or has not yet been provided.
    Unauthorized
    (models::ErrorModel)
    ,
    /// Forbidden. Authenticated user does not have the necessary permissions.
    Forbidden
    (models::ErrorModel)
    ,
    /// Credentials have timed out. If possible, the client should refresh credentials and retry.
    CredentialsHaveTimedOut
    (models::ErrorModel)
    ,
    /// The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry.
    TheServiceIsNotReadyToHandleTheRequest
    (models::ErrorModel)
    ,
    /// A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.
    AServer
    (models::ErrorModel)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum GetTokenResponse {
    /// OAuth2 token response for client credentials or token exchange
    OAuth
    (models::GetToken200Response)
    ,
    /// OAuth2 error response
    OAuth_2
    (models::GetToken400Response)
    ,
    /// OAuth2 error response
    OAuth_3
    (models::GetToken400Response)
    ,
    /// OAuth2 error response
    OAuth_4
    (models::GetToken400Response)
}

/// API
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait Api<C: Send + Sync> {
    fn poll_ready(&self, _cx: &mut Context) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>> {
        Poll::Ready(Ok(()))
    }

    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        context: &C) -> Result<CreateNamespaceResponse, ApiError>;

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        context: &C) -> Result<CreateTableResponse, ApiError>;

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        context: &C) -> Result<DropNamespaceResponse, ApiError>;

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        purge_requested: Option<bool>,
        context: &C) -> Result<DropTableResponse, ApiError>;

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        parent: Option<String>,
        context: &C) -> Result<ListNamespacesResponse, ApiError>;

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        context: &C) -> Result<ListTablesResponse, ApiError>;

    /// Load the metadata properties for a namespace
    async fn load_namespace_metadata(
        &self,
        prefix: String,
        namespace: String,
        context: &C) -> Result<LoadNamespaceMetadataResponse, ApiError>;

    /// Load a table from the catalog
    async fn load_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        context: &C) -> Result<LoadTableResponse, ApiError>;

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        context: &C) -> Result<RenameTableResponse, ApiError>;

    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        report_metrics_request: models::ReportMetricsRequest,
        context: &C) -> Result<ReportMetricsResponse, ApiError>;

    /// Check if a table exists
    async fn table_exists(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        context: &C) -> Result<TableExistsResponse, ApiError>;

    /// Set or remove properties on a namespace
    async fn update_properties(
        &self,
        prefix: String,
        namespace: String,
        update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest>,
        context: &C) -> Result<UpdatePropertiesResponse, ApiError>;

    /// Commit updates to a table
    async fn update_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        commit_table_request: Option<models::CommitTableRequest>,
        context: &C) -> Result<UpdateTableResponse, ApiError>;

    /// List all catalog configuration settings
    async fn get_config(
        &self,
        context: &C) -> Result<GetConfigResponse, ApiError>;

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
        context: &C) -> Result<GetTokenResponse, ApiError>;

}

/// API where `Context` isn't passed on every API call
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait ApiNoContext<C: Send + Sync> {

    fn poll_ready(&self, _cx: &mut Context) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>>;

    fn context(&self) -> &C;

    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        ) -> Result<CreateNamespaceResponse, ApiError>;

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        ) -> Result<CreateTableResponse, ApiError>;

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<DropNamespaceResponse, ApiError>;

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        purge_requested: Option<bool>,
        ) -> Result<DropTableResponse, ApiError>;

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        parent: Option<String>,
        ) -> Result<ListNamespacesResponse, ApiError>;

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<ListTablesResponse, ApiError>;

    /// Load the metadata properties for a namespace
    async fn load_namespace_metadata(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<LoadNamespaceMetadataResponse, ApiError>;

    /// Load a table from the catalog
    async fn load_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        ) -> Result<LoadTableResponse, ApiError>;

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        ) -> Result<RenameTableResponse, ApiError>;

    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        report_metrics_request: models::ReportMetricsRequest,
        ) -> Result<ReportMetricsResponse, ApiError>;

    /// Check if a table exists
    async fn table_exists(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        ) -> Result<TableExistsResponse, ApiError>;

    /// Set or remove properties on a namespace
    async fn update_properties(
        &self,
        prefix: String,
        namespace: String,
        update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest>,
        ) -> Result<UpdatePropertiesResponse, ApiError>;

    /// Commit updates to a table
    async fn update_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        commit_table_request: Option<models::CommitTableRequest>,
        ) -> Result<UpdateTableResponse, ApiError>;

    /// List all catalog configuration settings
    async fn get_config(
        &self,
        ) -> Result<GetConfigResponse, ApiError>;

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
        ) -> Result<GetTokenResponse, ApiError>;

}

/// Trait to extend an API to make it easy to bind it to a context.
pub trait ContextWrapperExt<C: Send + Sync> where Self: Sized
{
    /// Binds this API to a context.
    fn with_context(self, context: C) -> ContextWrapper<Self, C>;
}

impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ContextWrapperExt<C> for T {
    fn with_context(self: T, context: C) -> ContextWrapper<T, C> {
         ContextWrapper::<T, C>::new(self, context)
    }
}

#[async_trait]
impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ApiNoContext<C> for ContextWrapper<T, C> {
    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), ServiceError>> {
        self.api().poll_ready(cx)
    }

    fn context(&self) -> &C {
        ContextWrapper::context(self)
    }

    /// Create a namespace
    async fn create_namespace(
        &self,
        prefix: String,
        create_namespace_request: Option<models::CreateNamespaceRequest>,
        ) -> Result<CreateNamespaceResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().create_namespace(prefix, create_namespace_request, &context).await
    }

    /// Create a table in the given namespace
    async fn create_table(
        &self,
        prefix: String,
        namespace: String,
        create_table_request: Option<models::CreateTableRequest>,
        ) -> Result<CreateTableResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().create_table(prefix, namespace, create_table_request, &context).await
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<DropNamespaceResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().drop_namespace(prefix, namespace, &context).await
    }

    /// Drop a table from the catalog
    async fn drop_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        purge_requested: Option<bool>,
        ) -> Result<DropTableResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().drop_table(prefix, namespace, table, purge_requested, &context).await
    }

    /// List namespaces, optionally providing a parent namespace to list underneath
    async fn list_namespaces(
        &self,
        prefix: String,
        parent: Option<String>,
        ) -> Result<ListNamespacesResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().list_namespaces(prefix, parent, &context).await
    }

    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<ListTablesResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().list_tables(prefix, namespace, &context).await
    }

    /// Load the metadata properties for a namespace
    async fn load_namespace_metadata(
        &self,
        prefix: String,
        namespace: String,
        ) -> Result<LoadNamespaceMetadataResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().load_namespace_metadata(prefix, namespace, &context).await
    }

    /// Load a table from the catalog
    async fn load_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        ) -> Result<LoadTableResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().load_table(prefix, namespace, table, &context).await
    }

    /// Rename a table from its current name to a new name
    async fn rename_table(
        &self,
        prefix: String,
        rename_table_request: models::RenameTableRequest,
        ) -> Result<RenameTableResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().rename_table(prefix, rename_table_request, &context).await
    }

    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        report_metrics_request: models::ReportMetricsRequest,
        ) -> Result<ReportMetricsResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().report_metrics(prefix, namespace, table, report_metrics_request, &context).await
    }

    /// Check if a table exists
    async fn table_exists(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        ) -> Result<TableExistsResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().table_exists(prefix, namespace, table, &context).await
    }

    /// Set or remove properties on a namespace
    async fn update_properties(
        &self,
        prefix: String,
        namespace: String,
        update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest>,
        ) -> Result<UpdatePropertiesResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().update_properties(prefix, namespace, update_namespace_properties_request, &context).await
    }

    /// Commit updates to a table
    async fn update_table(
        &self,
        prefix: String,
        namespace: String,
        table: String,
        commit_table_request: Option<models::CommitTableRequest>,
        ) -> Result<UpdateTableResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().update_table(prefix, namespace, table, commit_table_request, &context).await
    }

    /// List all catalog configuration settings
    async fn get_config(
        &self,
        ) -> Result<GetConfigResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().get_config(&context).await
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
        ) -> Result<GetTokenResponse, ApiError>
    {
        let context = self.context().clone();
        self.api().get_token(grant_type, scope, client_id, client_secret, requested_token_type, subject_token, subject_token_type, actor_token, actor_token_type, &context).await
    }

}


#[cfg(feature = "client")]
pub mod client;

// Re-export Client as a top-level name
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;

// Re-export router() as a top-level name
#[cfg(feature = "server")]
pub use self::server::Service;

#[cfg(feature = "server")]
pub mod context;

pub mod models;

#[cfg(any(feature = "client", feature = "server"))]
pub(crate) mod header;
