/*!
Error type for iceberg
*/

use arrow::error::ArrowError;
use thiserror::Error;

#[derive(Error, Debug)]
/// Iceberg error
pub enum Error {
    /// Invalid format
    #[error("{0} doesn't have the right format")]
    InvalidFormat(String),
    /// Type error
    #[error("Value {0} doesn't have the {1} type.")]
    Type(String, String),
    /// Schema error
    #[error("Column {0} not in schema {1}.")]
    Schema(String, String),
    /// Conversion error
    #[error("Failed to convert {0} to {1}.")]
    Conversion(String, String),
    /// Failed to decompress gzip data
    #[error("Failed to decompress gzip data: {0}")]
    Decompress(String),
    /// Not found
    #[error("{0} not found.")]
    NotFound(String),
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Not found in catalog
    #[error("Entity not found in catalog")]
    CatalogNotFound,
    /// External error
    #[error(transparent)]
    External(Box<dyn std::error::Error + Send + Sync>),
    /// Iceberg spec error
    #[error(transparent)]
    Iceberg(#[from] iceberg_rust_spec::error::Error),
    /// Arrow error
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet error
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Avro error
    #[error(transparent)]
    Avro(Box<apache_avro::Error>),
    /// Thrift error
    #[error(transparent)]
    Thrift(#[from] thrift::Error),
    /// sql parser error
    #[error(transparent)]
    SQLParser(#[from] sqlparser::parser::ParserError),
    /// Serde json
    #[error(transparent)]
    JSONSerde(#[from] serde_json::Error),
    /// Chrono parse
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    /// Url parse
    #[error(transparent)]
    Url(#[from] url::ParseError),
    /// Io error
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// Channel error
    #[error(transparent)]
    FuturesChannel(#[from] futures::channel::mpsc::SendError),
    /// Tokio error
    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),
    /// Objectstore error
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// Try from slice error
    #[error(transparent)]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    /// Try from int error
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// Utf8 error
    #[error(transparent)]
    UTF8(#[from] std::str::Utf8Error),
    /// from utf8 error
    #[error(transparent)]
    FromUTF8(#[from] std::string::FromUtf8Error),
    /// parse int error
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// derive builder
    #[error(transparent)]
    DeriveBuilder(#[from] derive_builder::UninitializedFieldError),
}

impl From<apache_avro::Error> for Error {
    fn from(err: apache_avro::Error) -> Self {
        Error::Avro(Box::new(err))
    }
}

impl From<Error> for ArrowError {
    fn from(value: Error) -> Self {
        ArrowError::from_external_error(Box::new(value))
    }
}
