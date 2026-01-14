use super::configuration_rewriter::ConfigurationRewriter;
use crate::apis::configuration;
use crate::apis::configuration::Configuration;
use async_trait::async_trait;
use iceberg_rust::error::Error;
use reqwest;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
struct AccessToken {
    token: String,
    refresh_before: SystemTime,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Debug, Clone)]
pub struct OAuth2ClientCredentials {
    pub server_uri: String,
    pub client_id: String,
    pub client_secret: String,
    pub audience: Option<String>,
    pub scope: Option<String>,

    access_token: Arc<Mutex<Option<AccessToken>>>,
}

impl OAuth2ClientCredentials {
    pub fn new(
        server_uri: String,
        client_id: String,
        client_secret: String,
        audience: Option<String>,
        scope: Option<String>,
    ) -> Self {
        OAuth2ClientCredentials {
            server_uri,
            client_id,
            client_secret,
            audience,
            scope,
            access_token: Arc::new(Mutex::new(None)),
        }
    }

    async fn fetch_token(&self) -> Result<AccessToken, Error> {
        let client = reqwest::Client::new();
        let mut req_builder = client.request(reqwest::Method::POST, &self.server_uri);
        let mut body: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
        body.insert("grant_type", "client_credentials".to_owned());
        body.insert("client_id", self.client_id.to_owned());
        body.insert("client_secret", self.client_secret.to_owned());

        if let Some(s) = &self.audience {
            body.insert("audience", s.to_owned());
        }

        if let Some(s) = &self.scope {
            body.insert("scope", s.to_owned());
        }

        req_builder = req_builder.form(&body);

        let req = req_builder
            .build()
            .map_err(|e| Error::External(Box::new(e)))?;

        let resp = client
            .execute(req)
            .await
            .map_err(|e| Error::External(Box::new(e)))?
            .error_for_status()
            .map_err(|e| Error::External(Box::new(e)))?;

        let content = resp
            .text()
            .await
            .map_err(|e| Error::External(Box::new(e)))?;

        let token_resp: TokenResponse = serde_json::from_str(&content)?;

        Ok(AccessToken {
            token: token_resp.access_token,
            refresh_before: SystemTime::now()
                .checked_add(Duration::from_secs((token_resp.expires_in * 70) / 100))
                .unwrap(),
        })
    }
}

#[async_trait]
impl ConfigurationRewriter for OAuth2ClientCredentials {
    async fn rewrite_configuration(&self, mut conf: Configuration) -> Result<Configuration, Error> {
        {
            let access_token = self.access_token.lock().unwrap();

            if let Some(s) = (*access_token).as_ref() {
                if s.refresh_before > SystemTime::now() {
                    conf.oauth_access_token = Some(s.token.clone());
                    return Ok(conf);
                }
            }
        }

        let token = self.fetch_token().await?;
        conf.oauth_access_token = Some(token.token.clone());
        *self.access_token.lock().unwrap() = Some(token);

        Ok(conf)
    }
}
