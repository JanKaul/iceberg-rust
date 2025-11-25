/*! Utils for converting standard Iceberg config formats to equivalent `object_store` options
*/

use crate::error::Error;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::{parse_url_opts, ObjectStore, ObjectStoreScheme, StaticCredentialProvider};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// AWS configs
const CLIENT_REGION: &str = "client.region";
const AWS_ACCESS_KEY_ID: &str = "s3.access-key-id";
const AWS_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const AWS_SESSION_TOKEN: &str = "s3.session-token";
const AWS_REGION: &str = "s3.region";
const AWS_ENDPOINT: &str = "s3.endpoint";
const AWS_ALLOW_ANONYMOUS: &str = "s3.allow-anonymous";

/// GCP configs
const GCS_BUCKET: &str = "gcs.bucket";
const GCS_CREDENTIALS_JSON: &str = "gcs.credentials-json";
const GCS_TOKEN: &str = "gcs.oauth2.token";

/// Azure configs
const AZURE_CONTAINER_NAME: &str = "azure.container-name";
const AZURE_ENDPOINT: &str = "azure.endpoint";
const AZURE_STORAGE_ACCESS_KEY: &str = "azure.access-key";
const AZURE_STORAGE_ACCOUNT_NAME: &str = "azure.account-name";

/// Parse the url and Iceberg format of variuos storage options into the equivalent `object_store`
/// options and build the corresponding `ObjectStore`.
pub fn object_store_from_config(
    url: Url,
    config: HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Error> {
    let store = match ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)? {
        (ObjectStoreScheme::AmazonS3, _) => {
            let mut builder = AmazonS3Builder::new().with_url(url);
            for (key, option) in config {
                let s3_key = match key.as_str() {
                    AWS_ACCESS_KEY_ID => AmazonS3ConfigKey::AccessKeyId,
                    AWS_SECRET_ACCESS_KEY => AmazonS3ConfigKey::SecretAccessKey,
                    AWS_SESSION_TOKEN => AmazonS3ConfigKey::Token,
                    CLIENT_REGION | AWS_REGION => AmazonS3ConfigKey::Region,
                    AWS_ENDPOINT => {
                        if option.starts_with("http://") {
                            // This is mainly used for testing, e.g. against MinIO
                            builder = builder.with_allow_http(true);
                        }
                        AmazonS3ConfigKey::Endpoint
                    }
                    AWS_ALLOW_ANONYMOUS => AmazonS3ConfigKey::SkipSignature,
                    _ => continue,
                };
                builder = builder.with_config(s3_key, option);
            }
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }

        (ObjectStoreScheme::GoogleCloudStorage, _) => {
            let mut builder = GoogleCloudStorageBuilder::new().with_url(url);
            for (key, option) in config {
                let gcs_key = match key.as_str() {
                    GCS_CREDENTIALS_JSON => GoogleConfigKey::ServiceAccountKey,
                    GCS_BUCKET => GoogleConfigKey::Bucket,
                    GCS_TOKEN => {
                        let credential = GcpCredential { bearer: option };
                        let credential_provider =
                            Arc::new(StaticCredentialProvider::new(credential)) as _;
                        builder = builder.with_credentials(credential_provider);
                        continue;
                    }
                    _ => continue,
                };
                builder = builder.with_config(gcs_key, option);
            }
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }

        (ObjectStoreScheme::MicrosoftAzure, _) => {
            let mut builder = MicrosoftAzureBuilder::new().with_url(url);
            for (key, option) in config {
                let azure_key = match key.as_str() {
                    AZURE_CONTAINER_NAME => AzureConfigKey::ContainerName,
                    AZURE_STORAGE_ACCOUNT_NAME => AzureConfigKey::AccountName,
                    AZURE_STORAGE_ACCESS_KEY => AzureConfigKey::AccessKey,
                    AZURE_ENDPOINT => {
                        if option.starts_with("http://") {
                            // This is mainly used for testing, e.g. against Azurite
                            builder = builder.with_allow_http(true);
                        }
                        AzureConfigKey::Endpoint
                    }
                    _ => continue,
                };
                builder = builder.with_config(azure_key, option);
            }
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }

        _ => {
            let (store, _path) = parse_url_opts(&url, config)?;
            store.into()
        }
    };

    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use url::Url;

    #[test]
    fn test_s3_config_basic() {
        let url = Url::parse("s3://test-bucket/path").unwrap();
        let mut config = HashMap::new();
        config.insert(AWS_ACCESS_KEY_ID.to_string(), "test-key".to_string());
        config.insert(AWS_SECRET_ACCESS_KEY.to_string(), "test-secret".to_string());
        config.insert(AWS_SESSION_TOKEN.to_string(), "test-session".to_string());
        config.insert(AWS_REGION.to_string(), "us-east-1".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        assert!(store_repr.contains("region: \"us-east-1\""));
        assert!(store_repr.contains("bucket: \"test-bucket\""));
        assert!(store_repr.contains("key_id: \"test-key\""));
        assert!(store_repr.contains("secret_key: \"******\""));
        assert!(store_repr.contains("token: Some(\"******\")"));
        assert!(store_repr.contains("endpoint: None"));
        assert!(store_repr.contains("allow_http: Parsed(false)"));
        assert!(store_repr.contains("skip_signature: false"));
    }

    #[test]
    fn test_s3_config_with_http_endpoint() {
        let url = Url::parse("s3://test-bucket/").unwrap();
        let mut config = HashMap::new();
        config.insert(
            AWS_ENDPOINT.to_string(),
            "http://localhost:9000".to_string(),
        );
        config.insert(AWS_ALLOW_ANONYMOUS.to_string(), "true".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        assert!(store_repr.contains("region: \"us-east-1\""));
        assert!(store_repr.contains("bucket: \"test-bucket\""));
        assert!(!store_repr.contains("key_id: "));
        assert!(!store_repr.contains("secret_key: "));
        assert!(!store_repr.contains("token: "));
        assert!(store_repr.contains("endpoint: Some(\"http://localhost:9000\")"));
        assert!(store_repr.contains("allow_http: Parsed(true)"));
        assert!(store_repr.contains("skip_signature: true"));
    }

    #[test]
    fn test_gcs_config_with_service_account() {
        let url = Url::parse("gs://test-bucket/").unwrap();
        let mut config = HashMap::new();
        config.insert(
            GCS_CREDENTIALS_JSON.to_string(),
            json!(
                {
                  "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""
                }
            )
            .to_string(),
        );
        config.insert(GCS_BUCKET.to_string(), "test-bucket".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        assert!(store_repr.contains("bearer: \"\""));
        assert!(store_repr.contains("bucket_name: \"test-bucket\""));
    }

    #[test]
    fn test_gcs_config_with_oauth_token() {
        let url = Url::parse("gs://test-bucket/").unwrap();
        let mut config = HashMap::new();
        config.insert(GCS_TOKEN.to_string(), "oauth-token-123".to_string());
        config.insert(GCS_BUCKET.to_string(), "test-bucket".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        assert!(store_repr.contains("bearer: \"oauth-token-123\""));
        assert!(store_repr.contains("bucket_name: \"test-bucket\""));
    }

    #[test]
    fn test_azure_config_basic() {
        let url = Url::parse("https://testaccount.blob.core.windows.net/test-container").unwrap();
        let mut config = HashMap::new();
        config.insert(
            AZURE_STORAGE_ACCOUNT_NAME.to_string(),
            "testaccount".to_string(),
        );
        config.insert(AZURE_STORAGE_ACCESS_KEY.to_string(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        println!("{}", store_repr);
        assert!(store_repr.contains("account: \"testaccount\""));
        assert!(store_repr.contains("container: \"test-container\""));
        assert!(store_repr.contains("host: Some(Domain(\"testaccount.blob.core.windows.net\"))"));
        assert!(store_repr.contains("port: None"));
        assert!(store_repr.contains("scheme: \"https\""));
        assert!(store_repr.contains("allow_http: Parsed(false)"));
    }

    #[test]
    fn test_azure_config_with_http_endpoint() {
        let url = Url::parse("https://testaccount.blob.core.windows.net/test-container").unwrap();
        let mut config = HashMap::new();
        config.insert(
            AZURE_ENDPOINT.to_string(),
            "http://localhost:9000".to_string(),
        );
        config.insert(
            AZURE_STORAGE_ACCOUNT_NAME.to_string(),
            "testaccount".to_string(),
        );
        config.insert(AZURE_STORAGE_ACCESS_KEY.to_string(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string());

        let store = object_store_from_config(url, config).unwrap();
        let store_repr = format!("{store:?}");

        assert!(store_repr.contains("account: \"testaccount\""));
        assert!(store_repr.contains("container: \"test-container\""));
        assert!(store_repr.contains("host: Some(Domain(\"localhost\"))"));
        assert!(store_repr.contains("port: Some(9000)"));
        assert!(store_repr.contains("scheme: \"http\""));
        assert!(store_repr.contains("allow_http: Parsed(true)"));
    }
}
