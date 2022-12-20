use futures::{future, future::BoxFuture, future::FutureExt, stream, stream::TryStreamExt, Stream};
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::{Body, HeaderMap, Request, Response, StatusCode};
use log::warn;
#[allow(unused_imports)]
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
pub use swagger::auth::Authorization;
use swagger::auth::Scopes;
use swagger::{ApiError, BodyExt, Has, RequestParser, XSpanIdString};
use url::form_urlencoded;

use crate::header;
#[allow(unused_imports)]
use crate::models;

pub use crate::context;

type ServiceFuture = BoxFuture<'static, Result<Response<Body>, crate::ServiceError>>;

use crate::{
    Api, CreateNamespaceResponse, CreateTableResponse, DropNamespaceResponse, DropTableResponse,
    GetConfigResponse, GetTokenResponse, ListNamespacesResponse, ListTablesResponse,
    LoadNamespaceMetadataResponse, LoadTableResponse, RenameTableResponse, ReportMetricsResponse,
    TableExistsResponse, UpdatePropertiesResponse, UpdateTableResponse,
};

mod paths {
    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref GLOBAL_REGEX_SET: regex::RegexSet = regex::RegexSet::new(vec![
            r"^/v1/config$",
            r"^/v1/oauth/tokens$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/properties$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables/(?P<table>[^/?#]*)$",
            r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables/(?P<table>[^/?#]*)/metrics$",
            r"^/v1/(?P<prefix>[^/?#]*)/tables/rename$"
        ])
        .expect("Unable to create global regex set");
    }
    pub(crate) static ID_V1_CONFIG: usize = 0;
    pub(crate) static ID_V1_OAUTH_TOKENS: usize = 1;
    pub(crate) static ID_V1_PREFIX_NAMESPACES: usize = 2;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/v1/(?P<prefix>[^/?#]*)/namespaces$")
                .expect("Unable to create regex for V1_PREFIX_NAMESPACES");
    }
    pub(crate) static ID_V1_PREFIX_NAMESPACES_NAMESPACE: usize = 3;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES_NAMESPACE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)$")
                .expect("Unable to create regex for V1_PREFIX_NAMESPACES_NAMESPACE");
    }
    pub(crate) static ID_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES: usize = 4;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/properties$"
            )
            .expect("Unable to create regex for V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES");
    }
    pub(crate) static ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES: usize = 5;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables$"
            )
            .expect("Unable to create regex for V1_PREFIX_NAMESPACES_NAMESPACE_TABLES");
    }
    pub(crate) static ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE: usize = 6;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables/(?P<table>[^/?#]*)$")
                .expect("Unable to create regex for V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE");
    }
    pub(crate) static ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS: usize = 7;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/v1/(?P<prefix>[^/?#]*)/namespaces/(?P<namespace>[^/?#]*)/tables/(?P<table>[^/?#]*)/metrics$")
                .expect("Unable to create regex for V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS");
    }
    pub(crate) static ID_V1_PREFIX_TABLES_RENAME: usize = 8;
    lazy_static! {
        pub static ref REGEX_V1_PREFIX_TABLES_RENAME: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/v1/(?P<prefix>[^/?#]*)/tables/rename$")
                .expect("Unable to create regex for V1_PREFIX_TABLES_RENAME");
    }
}

pub struct MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    api_impl: T,
    marker: PhantomData<C>,
}

impl<T, C> MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    pub fn new(api_impl: T) -> Self {
        MakeService {
            api_impl,
            marker: PhantomData,
        }
    }
}

impl<T, C, Target> hyper::service::Service<Target> for MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    type Response = Service<T, C>;
    type Error = crate::ServiceError;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: Target) -> Self::Future {
        futures::future::ok(Service::new(self.api_impl.clone()))
    }
}

fn method_not_allowed() -> Result<Response<Body>, crate::ServiceError> {
    Ok(Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::empty())
        .expect("Unable to create Method Not Allowed response"))
}

pub struct Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    api_impl: T,
    marker: PhantomData<C>,
}

impl<T, C> Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    pub fn new(api_impl: T) -> Self {
        Service {
            api_impl,
            marker: PhantomData,
        }
    }
}

impl<T, C> Clone for Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Service {
            api_impl: self.api_impl.clone(),
            marker: self.marker,
        }
    }
}

impl<T, C> hyper::service::Service<(Request<Body>, C)> for Service<T, C>
where
    T: Api<C> + Clone + Send + Sync + 'static,
    C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = crate::ServiceError;
    type Future = ServiceFuture;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.api_impl.poll_ready(cx)
    }

    fn call(&mut self, req: (Request<Body>, C)) -> Self::Future {
        async fn run<T, C>(
            mut api_impl: T,
            req: (Request<Body>, C),
        ) -> Result<Response<Body>, crate::ServiceError>
        where
            T: Api<C> + Clone + Send + 'static,
            C: Has<XSpanIdString> + Has<Option<Authorization>> + Send + Sync + 'static,
        {
            let (request, context) = req;
            let (parts, body) = request.into_parts();
            let (method, uri, headers) = (parts.method, parts.uri, parts.headers);
            let path = paths::GLOBAL_REGEX_SET.matches(uri.path());

            match method {
                // CreateNamespace - POST /v1/{prefix}/namespaces
                hyper::Method::POST if path.matched(paths::ID_V1_PREFIX_NAMESPACES) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_create_namespace_request: Option<models::CreateNamespaceRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_create_namespace_request) => param_create_namespace_request,
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                };

                                let result = api_impl.create_namespace(
                                            param_prefix,
                                            param_create_namespace_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                CreateNamespaceResponse::RepresentsASuccessfulCallToCreateANamespace
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_REPRESENTS_A_SUCCESSFUL_CALL_TO_CREATE_A_NAMESPACE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::NotAcceptable
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(406).expect("Unable to turn 406 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_NOT_ACCEPTABLE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::Conflict
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(409).expect("Unable to turn 409 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_CONFLICT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateNamespaceResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_NAMESPACE_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter CreateNamespaceRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter CreateNamespaceRequest")),
                        }
                }

                // CreateTable - POST /v1/{prefix}/namespaces/{namespace}/tables
                hyper::Method::POST
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_create_table_request: Option<models::CreateTableRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_create_table_request) => param_create_table_request,
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                };

                                let result = api_impl.create_table(
                                            param_prefix,
                                            param_namespace,
                                            param_create_table_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                CreateTableResponse::TableMetadataResultAfterCreatingATable
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_TABLE_METADATA_RESULT_AFTER_CREATING_A_TABLE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::Conflict
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(409).expect("Unable to turn 409 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_CONFLICT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                CreateTableResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CREATE_TABLE_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter CreateTableRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter CreateTableRequest")),
                        }
                }

                // DropNamespace - DELETE /v1/{prefix}/namespaces/{namespace}
                hyper::Method::DELETE if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .drop_namespace(param_prefix, param_namespace, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            DropNamespaceResponse::Success => {
                                *response.status_mut() = StatusCode::from_u16(204)
                                    .expect("Unable to turn 204 into a StatusCode");
                            }
                            DropNamespaceResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::NotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_NOT_FOUND"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropNamespaceResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_NAMESPACE_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // DropTable - DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}
                hyper::Method::DELETE
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_table = match percent_encoding::percent_decode(path_params["table"].as_bytes()).decode_utf8() {
                    Ok(param_table) => match param_table.parse::<String>() {
                        Ok(param_table) => param_table,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter table: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["table"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_purge_requested = query_params
                        .iter()
                        .filter(|e| e.0 == "purgeRequested")
                        .map(|e| e.1.to_owned())
                        .next();
                    let param_purge_requested = match param_purge_requested {
                        Some(param_purge_requested) => {
                            let param_purge_requested =
                                <bool as std::str::FromStr>::from_str(&param_purge_requested);
                            match param_purge_requested {
                            Ok(param_purge_requested) => Some(param_purge_requested),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter purgeRequested - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter purgeRequested")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .drop_table(
                            param_prefix,
                            param_namespace,
                            param_table,
                            param_purge_requested,
                            &context,
                        )
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            DropTableResponse::Success => {
                                *response.status_mut() = StatusCode::from_u16(204)
                                    .expect("Unable to turn 204 into a StatusCode");
                            }
                            DropTableResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::NotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_NOT_FOUND"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DropTableResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DROP_TABLE_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // ListNamespaces - GET /v1/{prefix}/namespaces
                hyper::Method::GET if path.matched(paths::ID_V1_PREFIX_NAMESPACES) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_parent = query_params
                        .iter()
                        .filter(|e| e.0 == "parent")
                        .map(|e| e.1.to_owned())
                        .next();
                    let param_parent = match param_parent {
                        Some(param_parent) => {
                            let param_parent =
                                <String as std::str::FromStr>::from_str(&param_parent);
                            match param_parent {
                            Ok(param_parent) => Some(param_parent),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter parent - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter parent")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .list_namespaces(param_prefix, param_parent, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            ListNamespacesResponse::AListOfNamespaces(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_A_LIST_OF_NAMESPACES"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::NotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_NOT_FOUND"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::TheServiceIsNotReadyToHandleTheRequest(
                                body,
                            ) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListNamespacesResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_NAMESPACES_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // ListTables - GET /v1/{prefix}/namespaces/{namespace}/tables
                hyper::Method::GET
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .list_tables(param_prefix, param_namespace, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            ListTablesResponse::AListOfTableIdentifiers(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_A_LIST_OF_TABLE_IDENTIFIERS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::NotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_NOT_FOUND"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            ListTablesResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIST_TABLES_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // LoadNamespaceMetadata - GET /v1/{prefix}/namespaces/{namespace}
                hyper::Method::GET if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .load_namespace_metadata(param_prefix, param_namespace, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                                            Ok(rsp) => match rsp {
                                                LoadNamespaceMetadataResponse::ReturnsANamespace
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_RETURNS_A_NAMESPACE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                LoadNamespaceMetadataResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_NAMESPACE_METADATA_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                    Ok(response)
                }

                // LoadTable - GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
                hyper::Method::GET
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_table = match percent_encoding::percent_decode(path_params["table"].as_bytes()).decode_utf8() {
                    Ok(param_table) => match param_table.parse::<String>() {
                        Ok(param_table) => param_table,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter table: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["table"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .load_table(param_prefix, param_namespace, param_table, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            LoadTableResponse::TableMetadataResultWhenLoadingATable(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_TABLE_METADATA_RESULT_WHEN_LOADING_A_TABLE"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::NotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_NOT_FOUND"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            LoadTableResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LOAD_TABLE_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // RenameTable - POST /v1/{prefix}/tables/rename
                hyper::Method::POST if path.matched(paths::ID_V1_PREFIX_TABLES_RENAME) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_TABLES_RENAME
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_TABLES_RENAME in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_TABLES_RENAME.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_rename_table_request: Option<models::RenameTableRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_rename_table_request) => param_rename_table_request,
                                        Err(e) => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from(format!("Couldn't parse body parameter RenameTableRequest - doesn't match schema: {}", e)))
                                                        .expect("Unable to create Bad Request response for invalid body parameter RenameTableRequest due to schema")),
                                    }
                                } else {
                                    None
                                };
                                let param_rename_table_request = match param_rename_table_request {
                                    Some(param_rename_table_request) => param_rename_table_request,
                                    None => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Missing required body parameter RenameTableRequest"))
                                                        .expect("Unable to create Bad Request response for missing body parameter RenameTableRequest")),
                                };

                                let result = api_impl.rename_table(
                                            param_prefix,
                                            param_rename_table_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                RenameTableResponse::OK
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                },
                                                RenameTableResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::NotAcceptable
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(406).expect("Unable to turn 406 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_NOT_ACCEPTABLE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::Conflict
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(409).expect("Unable to turn 409 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_CONFLICT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                RenameTableResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for RENAME_TABLE_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter RenameTableRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter RenameTableRequest")),
                        }
                }

                // ReportMetrics - POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
                hyper::Method::POST
                    if path
                        .matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_table = match percent_encoding::percent_decode(path_params["table"].as_bytes()).decode_utf8() {
                    Ok(param_table) => match param_table.parse::<String>() {
                        Ok(param_table) => param_table,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter table: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["table"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_report_metrics_request: Option<models::ReportMetricsRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_report_metrics_request) => param_report_metrics_request,
                                        Err(e) => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from(format!("Couldn't parse body parameter ReportMetricsRequest - doesn't match schema: {}", e)))
                                                        .expect("Unable to create Bad Request response for invalid body parameter ReportMetricsRequest due to schema")),
                                    }
                                } else {
                                    None
                                };
                                let param_report_metrics_request = match param_report_metrics_request {
                                    Some(param_report_metrics_request) => param_report_metrics_request,
                                    None => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Missing required body parameter ReportMetricsRequest"))
                                                        .expect("Unable to create Bad Request response for missing body parameter ReportMetricsRequest")),
                                };

                                let result = api_impl.report_metrics(
                                            param_prefix,
                                            param_namespace,
                                            param_table,
                                            param_report_metrics_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                ReportMetricsResponse::Success
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(204).expect("Unable to turn 204 into a StatusCode");
                                                },
                                                ReportMetricsResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                ReportMetricsResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for REPORT_METRICS_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter ReportMetricsRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter ReportMetricsRequest")),
                        }
                }

                // TableExists - HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}
                hyper::Method::HEAD
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_table = match percent_encoding::percent_decode(path_params["table"].as_bytes()).decode_utf8() {
                    Ok(param_table) => match param_table.parse::<String>() {
                        Ok(param_table) => param_table,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter table: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["table"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .table_exists(param_prefix, param_namespace, param_table, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            TableExistsResponse::OK => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                            }
                            TableExistsResponse::BadRequest => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                            }
                            TableExistsResponse::Unauthorized => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                            }
                            TableExistsResponse::NotFound => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                            }
                            TableExistsResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for TABLE_EXISTS_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            TableExistsResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for TABLE_EXISTS_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            TableExistsResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for TABLE_EXISTS_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // UpdateProperties - POST /v1/{prefix}/namespaces/{namespace}/properties
                hyper::Method::POST
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_update_namespace_properties_request: Option<models::UpdateNamespacePropertiesRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_update_namespace_properties_request) => param_update_namespace_properties_request,
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                };

                                let result = api_impl.update_properties(
                                            param_prefix,
                                            param_namespace,
                                            param_update_namespace_properties_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                UpdatePropertiesResponse::JSONDataResponseForASynchronousUpdatePropertiesRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_JSON_DATA_RESPONSE_FOR_A_SYNCHRONOUS_UPDATE_PROPERTIES_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::NotAcceptable
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(406).expect("Unable to turn 406 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_NOT_ACCEPTABLE"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::UnprocessableEntity
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(422).expect("Unable to turn 422 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_UNPROCESSABLE_ENTITY"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdatePropertiesResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_PROPERTIES_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter UpdateNamespacePropertiesRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter UpdateNamespacePropertiesRequest")),
                        }
                }

                // UpdateTable - POST /v1/{prefix}/namespaces/{namespace}/tables/{table}
                hyper::Method::POST
                    if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
                {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE in set but failed match against \"{}\"", path, paths::REGEX_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE.as_str())
                    );

                    let param_prefix = match percent_encoding::percent_decode(path_params["prefix"].as_bytes()).decode_utf8() {
                    Ok(param_prefix) => match param_prefix.parse::<String>() {
                        Ok(param_prefix) => param_prefix,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter prefix: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["prefix"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_namespace = match percent_encoding::percent_decode(path_params["namespace"].as_bytes()).decode_utf8() {
                    Ok(param_namespace) => match param_namespace.parse::<String>() {
                        Ok(param_namespace) => param_namespace,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter namespace: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["namespace"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_table = match percent_encoding::percent_decode(path_params["table"].as_bytes()).decode_utf8() {
                    Ok(param_table) => match param_table.parse::<String>() {
                        Ok(param_table) => param_table,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter table: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["table"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_commit_table_request: Option<models::CommitTableRequest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&*body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_commit_table_request) => param_commit_table_request,
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                };

                                let result = api_impl.update_table(
                                            param_prefix,
                                            param_namespace,
                                            param_table,
                                            param_commit_table_request,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                UpdateTableResponse::ResponseUsedWhenATableIsSuccessfullyUpdated
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_RESPONSE_USED_WHEN_A_TABLE_IS_SUCCESSFULLY_UPDATED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::IndicatesABadRequestError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_INDICATES_A_BAD_REQUEST_ERROR"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::Unauthorized
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(401).expect("Unable to turn 401 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_UNAUTHORIZED"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::Forbidden
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(403).expect("Unable to turn 403 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_FORBIDDEN"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::NotFound
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(404).expect("Unable to turn 404 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_NOT_FOUND"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::Conflict
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(409).expect("Unable to turn 409 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_CONFLICT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::CredentialsHaveTimedOut
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(419).expect("Unable to turn 419 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_CREDENTIALS_HAVE_TIMED_OUT"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::AnUnknownServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 500 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_AN_UNKNOWN_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::TheServiceIsNotReadyToHandleTheRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(503).expect("Unable to turn 503 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::AServer
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(504).expect("Unable to turn 504 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_A_SERVER"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                UpdateTableResponse::AServer_2
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 5XX into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for UPDATE_TABLE_A_SERVER_2"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from(format!("Couldn't read body parameter CommitTableRequest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter CommitTableRequest")),
                        }
                }

                // GetConfig - GET /v1/config
                hyper::Method::GET if path.matched(paths::ID_V1_CONFIG) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    let result = api_impl.get_config(&context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            GetConfigResponse::ServerSpecifiedConfigurationValues(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_SERVER_SPECIFIED_CONFIGURATION_VALUES"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::IndicatesABadRequestError(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_INDICATES_A_BAD_REQUEST_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::Unauthorized(body) => {
                                *response.status_mut() = StatusCode::from_u16(401)
                                    .expect("Unable to turn 401 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_UNAUTHORIZED"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::Forbidden(body) => {
                                *response.status_mut() = StatusCode::from_u16(403)
                                    .expect("Unable to turn 403 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_FORBIDDEN"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::CredentialsHaveTimedOut(body) => {
                                *response.status_mut() = StatusCode::from_u16(419)
                                    .expect("Unable to turn 419 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_CREDENTIALS_HAVE_TIMED_OUT"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::TheServiceIsNotReadyToHandleTheRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(503)
                                    .expect("Unable to turn 503 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_THE_SERVICE_IS_NOT_READY_TO_HANDLE_THE_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            GetConfigResponse::AServer(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 5XX into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_CONFIG_A_SERVER"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // GetToken - POST /v1/oauth/tokens
                hyper::Method::POST if path.matched(paths::ID_V1_OAUTH_TOKENS) => {
                    {
                        let authorization = match *(&context as &dyn Has<Option<Authorization>>)
                            .get()
                        {
                            Some(ref authorization) => authorization,
                            None => {
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from("Unauthenticated"))
                                    .expect("Unable to create Authentication Forbidden response"))
                            }
                        };

                        // Authorization
                        if let Scopes::Some(ref scopes) = authorization.scopes {
                            let required_scopes: std::collections::BTreeSet<String> = vec![
                                "catalog".to_string(), // Allows interacting with the Config and Catalog APIs
                            ]
                            .into_iter()
                            .collect();

                            if !required_scopes.is_subset(scopes) {
                                let missing_scopes = required_scopes.difference(scopes);
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::from(missing_scopes.fold(
                                        "Insufficient authorization, missing scopes".to_string(),
                                        |s, scope| format!("{} {}", s, scope),
                                    )))
                                    .expect(
                                        "Unable to create Authentication Insufficient response",
                                    ));
                            }
                        }
                    }

                    // Form parameters
                    let param_grant_type = Some("grant_type_example".to_string());
                    let param_scope = Some("scope_example".to_string());
                    let param_client_id = Some("client_id_example".to_string());
                    let param_client_secret = Some("client_secret_example".to_string());
                    let param_requested_token_type = None;
                    let param_subject_token = Some("subject_token_example".to_string());
                    let param_subject_token_type = None;
                    let param_actor_token = Some("actor_token_example".to_string());
                    let param_actor_token_type = None;

                    let result = api_impl
                        .get_token(
                            param_grant_type,
                            param_scope,
                            param_client_id,
                            param_client_secret,
                            param_requested_token_type,
                            param_subject_token,
                            param_subject_token_type,
                            param_actor_token,
                            param_actor_token_type,
                            &context,
                        )
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => {
                            match rsp {
                                GetTokenResponse::OAuth(body) => {
                                    *response.status_mut() = StatusCode::from_u16(200)
                                        .expect("Unable to turn 200 into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_TOKEN_O_AUTH"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                                GetTokenResponse::OAuth_2(body) => {
                                    *response.status_mut() = StatusCode::from_u16(400)
                                        .expect("Unable to turn 400 into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_TOKEN_O_AUTH_2"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                                GetTokenResponse::OAuth_3(body) => {
                                    *response.status_mut() = StatusCode::from_u16(401)
                                        .expect("Unable to turn 401 into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_TOKEN_O_AUTH_3"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                                GetTokenResponse::OAuth_4(body) => {
                                    *response.status_mut() = StatusCode::from_u16(500)
                                        .expect("Unable to turn 5XX into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for GET_TOKEN_O_AUTH_4"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                            }
                        }
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                _ if path.matched(paths::ID_V1_CONFIG) => method_not_allowed(),
                _ if path.matched(paths::ID_V1_OAUTH_TOKENS) => method_not_allowed(),
                _ if path.matched(paths::ID_V1_PREFIX_NAMESPACES) => method_not_allowed(),
                _ if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE) => method_not_allowed(),
                _ if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES) => {
                    method_not_allowed()
                }
                _ if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES) => {
                    method_not_allowed()
                }
                _ if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) => {
                    method_not_allowed()
                }
                _ if path
                    .matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS) =>
                {
                    method_not_allowed()
                }
                _ if path.matched(paths::ID_V1_PREFIX_TABLES_RENAME) => method_not_allowed(),
                _ => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("Unable to create Not Found response")),
            }
        }
        Box::pin(run(self.api_impl.clone(), req))
    }
}

/// Request parser for `Api`.
pub struct ApiRequestParser;
impl<T> RequestParser<T> for ApiRequestParser {
    fn parse_operation_id(request: &Request<T>) -> Option<&'static str> {
        let path = paths::GLOBAL_REGEX_SET.matches(request.uri().path());
        match *request.method() {
            // CreateNamespace - POST /v1/{prefix}/namespaces
            hyper::Method::POST if path.matched(paths::ID_V1_PREFIX_NAMESPACES) => {
                Some("CreateNamespace")
            }
            // CreateTable - POST /v1/{prefix}/namespaces/{namespace}/tables
            hyper::Method::POST
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES) =>
            {
                Some("CreateTable")
            }
            // DropNamespace - DELETE /v1/{prefix}/namespaces/{namespace}
            hyper::Method::DELETE if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE) => {
                Some("DropNamespace")
            }
            // DropTable - DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}
            hyper::Method::DELETE
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
            {
                Some("DropTable")
            }
            // ListNamespaces - GET /v1/{prefix}/namespaces
            hyper::Method::GET if path.matched(paths::ID_V1_PREFIX_NAMESPACES) => {
                Some("ListNamespaces")
            }
            // ListTables - GET /v1/{prefix}/namespaces/{namespace}/tables
            hyper::Method::GET if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES) => {
                Some("ListTables")
            }
            // LoadNamespaceMetadata - GET /v1/{prefix}/namespaces/{namespace}
            hyper::Method::GET if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE) => {
                Some("LoadNamespaceMetadata")
            }
            // LoadTable - GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
            hyper::Method::GET
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
            {
                Some("LoadTable")
            }
            // RenameTable - POST /v1/{prefix}/tables/rename
            hyper::Method::POST if path.matched(paths::ID_V1_PREFIX_TABLES_RENAME) => {
                Some("RenameTable")
            }
            // ReportMetrics - POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
            hyper::Method::POST
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE_METRICS) =>
            {
                Some("ReportMetrics")
            }
            // TableExists - HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}
            hyper::Method::HEAD
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
            {
                Some("TableExists")
            }
            // UpdateProperties - POST /v1/{prefix}/namespaces/{namespace}/properties
            hyper::Method::POST
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_PROPERTIES) =>
            {
                Some("UpdateProperties")
            }
            // UpdateTable - POST /v1/{prefix}/namespaces/{namespace}/tables/{table}
            hyper::Method::POST
                if path.matched(paths::ID_V1_PREFIX_NAMESPACES_NAMESPACE_TABLES_TABLE) =>
            {
                Some("UpdateTable")
            }
            // GetConfig - GET /v1/config
            hyper::Method::GET if path.matched(paths::ID_V1_CONFIG) => Some("GetConfig"),
            // GetToken - POST /v1/oauth/tokens
            hyper::Method::POST if path.matched(paths::ID_V1_OAUTH_TOKENS) => Some("GetToken"),
            _ => None,
        }
    }
}
