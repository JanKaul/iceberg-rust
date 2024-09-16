use iceberg_rust::error::Error;

use crate::apis::{self, catalog_api_api::CreateNamespaceError};

/**
Error conversion
*/

impl<T> From<apis::Error<T>> for Error {
    fn from(val: apis::Error<T>) -> Self {
        match val {
            apis::Error::Reqwest(err) => Error::InvalidFormat(err.to_string()),
            apis::Error::ReqwestMiddleware(err) => Error::InvalidFormat(err.to_string()),
            apis::Error::Serde(err) => Error::JSONSerde(err),
            apis::Error::Io(err) => Error::IO(err),
            apis::Error::ResponseError(err) => Error::InvalidFormat(format!(
                "Response status: {}, Response content: {}",
                err.status, err.content
            )),
        }
    }
}
