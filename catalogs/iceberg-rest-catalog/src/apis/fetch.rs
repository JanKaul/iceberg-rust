use crate::apis::{configuration, ResponseContent};

use super::Error;

use std::collections::HashMap;

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
    T: for<'a> serde::Deserialize<'a>,
    E: for<'a> serde::Deserialize<'a>,
{
    let uri_base = match prefix {
        Some(prefix) => format!(
            "{}/v1/{prefix}/",
            configuration.base_path,
            prefix = crate::apis::urlencode(prefix)
        ),
        None => format!("{}/v1/", configuration.base_path,),
    };
    let client = &configuration.client;

    let uri = uri_base + uri_str;
    let mut req_builder = client.request(method.clone(), &uri);

    if let Some(ref aws_v4_key) = configuration.aws_v4_key {
        let new_headers = match aws_v4_key.sign(
            &uri,
            method.as_str(),
            &serde_json::to_string(&request).expect("param should serialize to string"),
        ) {
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
    if let Some(ref token) = configuration.oauth_access_token {
        req_builder = req_builder.bearer_auth(token.to_owned());
    };
    if let Some(ref token) = configuration.bearer_access_token {
        req_builder = req_builder.bearer_auth(token.to_owned());
    };
    for (key, value) in headers.unwrap_or_default() {
        req_builder = req_builder.header(key, value);
    }
    for (key, value) in query_params.unwrap_or_default() {
        req_builder = req_builder.query(&[(key, value)]);
    }
    if let &reqwest::Method::POST | &reqwest::Method::PUT = &method {
        req_builder = req_builder.json(request);
    }

    let req = req_builder.build()?;
    let resp = client.execute(req).await?;

    let status = resp.status();
    let content = resp.text().await?;

    if !status.is_client_error() && !status.is_server_error() {
        serde_json::from_str(&content).map_err(Error::from)
    } else {
        let entity: Option<E> = serde_json::from_str(&content).ok();
        let error = ResponseContent {
            status,
            content,
            entity,
        };
        Err(Error::ResponseError(error))
    }
}

pub(crate) async fn fetch_empty<R, E>(
    configuration: &configuration::Configuration,
    method: reqwest::Method,
    prefix: Option<&str>,
    uri_str: &str,
    request: &R,
    headers: Option<HashMap<String, String>>,
    query_params: Option<HashMap<String, String>>,
) -> Result<(), Error<E>>
where
    R: serde::Serialize + ?Sized,
    E: for<'a> serde::Deserialize<'a>,
{
    let uri_base = match prefix {
        Some(prefix) => format!(
            "{}/v1/{prefix}/",
            configuration.base_path,
            prefix = crate::apis::urlencode(prefix)
        ),
        None => format!("{}/v1/", configuration.base_path,),
    };
    let client = &configuration.client;

    let uri = uri_base + uri_str;
    let mut req_builder = client.request(method.clone(), &uri);

    if let Some(ref aws_v4_key) = configuration.aws_v4_key {
        let new_headers = match aws_v4_key.sign(
            &uri,
            method.as_str(),
            &serde_json::to_string(&request).expect("param should serialize to string"),
        ) {
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
    if let Some(ref token) = configuration.oauth_access_token {
        req_builder = req_builder.bearer_auth(token.to_owned());
    };
    if let Some(ref token) = configuration.bearer_access_token {
        req_builder = req_builder.bearer_auth(token.to_owned());
    };
    for (key, value) in headers.unwrap_or_default() {
        req_builder = req_builder.header(key, value);
    }
    for (key, value) in query_params.unwrap_or_default() {
        req_builder = req_builder.query(&[(key, value)]);
    }
    if let &reqwest::Method::POST | &reqwest::Method::PUT = &method {
        req_builder = req_builder.json(request);
    }

    let req = req_builder.build()?;
    let resp = client.execute(req).await?;

    let status = resp.status();
    let content = resp.text().await?;

    if !status.is_client_error() && !status.is_server_error() {
        Ok(())
    } else {
        let entity: Option<E> = serde_json::from_str(&content).ok();
        let error = ResponseContent {
            status,
            content,
            entity,
        };
        Err(Error::ResponseError(error))
    }
}
