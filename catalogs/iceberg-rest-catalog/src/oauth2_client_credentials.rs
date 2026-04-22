use crate::apis::configuration::OAuthAccessTokenProvider;
use iceberg_rust::error::Error;
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

/// Create an [`OAuthAccessTokenProvider`] that fetches tokens using the OAuth2 client credentials
/// flow.
///
/// Tokens are cached and automatically refreshed when they expire (at 70% of the `expires_in`
/// duration).
///
/// # Example
///
/// ```rust,no_run
/// use iceberg_rest_catalog::apis::configuration::ConfigurationBuilder;
/// use iceberg_rest_catalog::oauth2_client_credentials::oauth2_client_credentials;
///
/// let configuration = ConfigurationBuilder::default()
///     .base_path("https://my-iceberg-catalog.example.com")
///     .oauth_access_token(oauth2_client_credentials(
///         "https://auth.example.com/oauth/token",
///         "my-client-id",
///         "my-client-secret",
///         Some("my-audience".to_owned()),
///         None,
///     ))
///     .build()
///     .unwrap();
/// ```
pub fn oauth2_client_credentials(
    server_uri: impl Into<String>,
    client_id: impl Into<String>,
    client_secret: impl Into<String>,
    audience: Option<String>,
    scope: Option<String>,
) -> OAuthAccessTokenProvider {
    let server_uri = server_uri.into();
    let client_id = client_id.into();
    let client_secret = client_secret.into();
    let access_token: Arc<Mutex<Option<AccessToken>>> = Arc::new(Mutex::new(None));

    Arc::new(move || {
        let server_uri = server_uri.clone();
        let client_id = client_id.clone();
        let client_secret = client_secret.clone();
        let audience = audience.clone();
        let scope = scope.clone();
        let access_token = access_token.clone();

        Box::pin(async move {
            {
                let cached = access_token.lock().unwrap();
                if let Some(s) = cached.as_ref() {
                    if s.refresh_before > SystemTime::now() {
                        return Ok(s.token.clone());
                    }
                }
            }

            let client = reqwest::Client::new();
            let mut body: std::collections::HashMap<&str, String> =
                std::collections::HashMap::new();
            body.insert("grant_type", "client_credentials".to_owned());
            body.insert("client_id", client_id);
            body.insert("client_secret", client_secret);

            if let Some(s) = &audience {
                body.insert("audience", s.to_owned());
            }

            if let Some(s) = &scope {
                body.insert("scope", s.to_owned());
            }

            let resp = client
                .post(&server_uri)
                .form(&body)
                .send()
                .await
                .map_err(|e| Error::External(Box::new(e)))?
                .error_for_status()
                .map_err(|e| Error::External(Box::new(e)))?;

            let content = resp
                .text()
                .await
                .map_err(|e| Error::External(Box::new(e)))?;

            let token_resp: TokenResponse = serde_json::from_str(&content)?;

            let token = AccessToken {
                token: token_resp.access_token,
                refresh_before: SystemTime::now()
                    .checked_add(Duration::from_secs((token_resp.expires_in * 70) / 100))
                    .unwrap(),
            };

            let result = token.token.clone();
            *access_token.lock().unwrap() = Some(token);

            Ok(result)
        })
    })
}
