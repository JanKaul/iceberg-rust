/*!
Error type for iceberg
*/

use datafusion::error::DataFusionError;
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
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Not found
    #[error("{0} {1} not found.")]
    NotFound(String, String),
    /// Datafusion error
    #[error("datafusion error")]
    Datafusion(#[from] datafusion::error::DataFusionError),
    /// Arrow error
    #[error("arrowerror")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// sql parser error
    #[error("sql parser error")]
    SQLParser(#[from] datafusion::sql::sqlparser::parser::ParserError),
    /// Iceberg error
    #[error("iceberg error")]
    Iceberg(#[from] iceberg_rust::error::Error),
    /// Serde json
    #[error("serde json error")]
    JSONSerde(#[from] serde_json::Error),
    /// Chrono parse
    #[error("chrono parse error")]
    Chrono(#[from] chrono::ParseError),
    /// Io error
    #[error("io error")]
    IO(#[from] std::io::Error),
    /// Channel error
    #[error("channel error")]
    FuturesChannel(#[from] futures::channel::mpsc::SendError),
    /// Objectstore error
    #[error("object store error")]
    ObjectStore(#[from] object_store::Error),
    /// Try from slice error
    #[error("try from slice error")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    /// Try from int error
    #[error("try from int error")]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// Utf8 error
    #[error("utf8 error")]
    UTF8(#[from] std::str::Utf8Error),
    /// from utf8 error
    #[error("from utf8 error")]
    FromUTF8(#[from] std::string::FromUtf8Error),
    /// parse int error
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
}

impl From<Error> for DataFusionError {
    fn from(value: Error) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
