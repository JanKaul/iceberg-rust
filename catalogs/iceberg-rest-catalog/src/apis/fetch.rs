use crate::apis::{configuration, ResponseContent};

use super::{Conditional, Error};

use std::collections::HashMap;

pub(crate) async fn fetch_inner<R, E>(
    configuration: &configuration::Configuration,
    method: reqwest::Method,
    prefix: Option<&str>,
    uri_str: &str,
    request: &R,
    headers: Option<HashMap<String, String>>,
    query_params: Option<HashMap<String, String>>,
) -> Result<reqwest::Response, Error<E>>
where
    R: serde::Serialize + ?Sized,
{
    let uri_base = build_uri_base(configuration, prefix);
    let client = &configuration.client;

    let uri = uri_base + uri_str;
    let mut req_builder = client.request(method.clone(), &uri);

    for (key, value) in query_params.unwrap_or_default() {
        req_builder = req_builder.query(&[(key, value)]);
    }

    if let Some(ref aws_v4_key) = configuration.aws_v4_key {
        let body_str = match serde_json::to_value(&request) {
            Ok(serde_json::Value::Null) => "",
            _ => &serde_json::to_string(&request).expect("param should serialize to string"),
        };
        let uri_for_signing = match req_builder.try_clone() {
            Some(cloned_builder) => match cloned_builder.build() {
                Ok(tmp_req) => tmp_req.url().as_str().to_string(),
                Err(_) => uri.clone(),
            },
            None => uri.clone(),
        };
        let new_headers = match aws_v4_key.sign(&uri_for_signing, method.as_str(), body_str) {
            Ok(new_headers) => new_headers,
            Err(err) => return Err(Error::AWSV4SignatureError(err)),
        };
        for (name, value) in new_headers.iter() {
            req_builder = req_builder.header(name.as_str(), value.as_str());
        }
    }
    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }
    if let Some(token) = configuration
        .resolve_oauth_token()
        .await
        .map_err(Error::OAuthToken)?
    {
        req_builder = req_builder.bearer_auth(token);
    };
    if let Some(ref token) = configuration.bearer_access_token {
        req_builder = req_builder.bearer_auth(token.to_owned());
    };
    for (key, value) in headers.unwrap_or_default() {
        req_builder = req_builder.header(key, value);
    }
    if let &reqwest::Method::POST | &reqwest::Method::PUT = &method {
        req_builder = req_builder.json(request);
    }

    let req = req_builder.build()?;
    client.execute(req).await.map_err(Error::from)
}

pub(crate) async fn fetch<R, T, E>(
    configuration: &configuration::Configuration,
    method: reqwest::Method,
    prefix: Option<&str>,
    uri_str: &str,
    request: &R,
    headers: Option<HashMap<String, String>>,
    query_params: Option<HashMap<String, String>>,
) -> Result<T, Error<E>>
where
    R: serde::Serialize + ?Sized,
    T: serde::de::DeserializeOwned,
    E: serde::de::DeserializeOwned,
{
    let resp = fetch_inner(
        configuration,
        method,
        prefix,
        uri_str,
        request,
        headers,
        query_params,
    )
    .await?;
    let status = resp.status();
    let content = resp.text().await?;
    if !status.is_client_error() && !status.is_server_error() {
        let body = if content.is_empty() { "null" } else { &content };
        serde_json::from_str(body).map_err(Error::from)
    } else {
        let entity: Option<E> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent {
            status,
            content,
            entity,
        }))
    }
}

pub(crate) async fn fetch_conditional<R, T, E>(
    configuration: &configuration::Configuration,
    method: reqwest::Method,
    prefix: Option<&str>,
    uri_str: &str,
    request: &R,
    headers: Option<HashMap<String, String>>,
    query_params: Option<HashMap<String, String>>,
) -> Result<Conditional<T>, Error<E>>
where
    R: serde::Serialize + ?Sized,
    T: serde::de::DeserializeOwned,
    E: serde::de::DeserializeOwned,
{
    let resp = fetch_inner(
        configuration,
        method,
        prefix,
        uri_str,
        request,
        headers,
        query_params,
    )
    .await?;
    if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(Conditional::NotModified);
    }
    let resp_headers = resp.headers().clone();
    let status = resp.status();
    let content = resp.text().await?;
    if !status.is_client_error() && !status.is_server_error() {
        let body = if content.is_empty() { "null" } else { &content };
        let value = serde_json::from_str(body).map_err(Error::from)?;
        Ok(Conditional::Modified {
            value,
            headers: resp_headers,
        })
    } else {
        let entity: Option<E> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent {
            status,
            content,
            entity,
        }))
    }
}

/// Build the base URI for REST API calls with proper prefix encoding
fn build_uri_base(configuration: &configuration::Configuration, prefix: Option<&str>) -> String {
    match prefix {
        Some(prefix) => {
            // Split prefix by '/' and URL-encode each segment individually
            // This allows paths like "catalogs/warehouse_name" while still protecting
            // against path traversal attacks (e.g., "../../../etc")
            let encoded_segments: Vec<String> = prefix
                .split('/')
                .map(|segment| crate::apis::urlencode(segment))
                .collect();
            format!(
                "{}/v1/{}/",
                configuration.base_path,
                encoded_segments.join("/")
            )
        }
        None => format!("{}/v1/", configuration.base_path),
    }
}
