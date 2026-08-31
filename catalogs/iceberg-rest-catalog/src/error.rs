use iceberg_rust::error::Error;
use reqwest::StatusCode;

use crate::apis::{self, catalog_api_api::CreateNamespaceError, ResponseContent};

pub(crate) fn commit_error<T>(error: apis::Error<T>, identifier: &str) -> Error {
    match error {
        apis::Error::ResponseError(ResponseContent {
            status: StatusCode::CONFLICT,
            ..
        }) => Error::CommitConflict(identifier.to_owned()),
        error => error.into(),
    }
}

/**
Error conversion
*/
impl<T> From<apis::Error<T>> for Error {
    fn from(val: apis::Error<T>) -> Self {
        match val {
            apis::Error::Reqwest(err) => Error::External(err.into()),
            apis::Error::Serde(err) => Error::JSONSerde(err),
            apis::Error::Io(err) => Error::IO(err),
            apis::Error::ResponseError(ResponseContent {
                status: StatusCode::NOT_FOUND,
                content,
                entity: _,
            }) => Error::NotFound(content),
            apis::Error::ResponseError(err) => Error::InvalidFormat(format!(
                "Response status: {}, Response content: {}",
                err.status, err.content
            )),
            apis::Error::AWSV4SignatureError(err) => Error::External(Box::new(err)),
            apis::Error::OAuthToken(err) => err,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_table_conflict_is_structured_commit_conflict() {
        let error = commit_error(
            apis::Error::<()>::ResponseError(ResponseContent {
                status: StatusCode::CONFLICT,
                content: "concurrent commit".to_string(),
                entity: None,
            }),
            "catalog.namespace.table",
        );

        assert!(
            matches!(error, Error::CommitConflict(identifier) if identifier == "catalog.namespace.table")
        );
    }

    #[test]
    fn generic_conflict_remains_a_response_error() {
        let error: Error = apis::Error::<()>::ResponseError(ResponseContent {
            status: StatusCode::CONFLICT,
            content: "concurrent commit".to_string(),
            entity: None,
        })
        .into();

        assert!(matches!(error, Error::InvalidFormat(_)));
    }
}
