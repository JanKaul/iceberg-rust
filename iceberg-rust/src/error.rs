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
    /// Not found
    #[error("{0} {1} not found.")]
    NotFound(String, String),
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Not found in catalog
    #[error("Entity not found in catalog")]
    CatalogNotFound,
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
    Avro(#[from] apache_avro::Error),
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
    /// table metadata builder
    #[error(transparent)]
    TableMetadataBuilder(
        #[from] iceberg_rust_spec::spec::table_metadata::TableMetadataBuilderError,
    ),
    /// view metadata builder
    #[error(transparent)]
    ViewMetadataBuilder(
        #[from] iceberg_rust_spec::spec::view_metadata::GeneralViewMetadataBuilderError,
    ),
    /// version builder
    #[error(transparent)]
    VersionBuilder(#[from] iceberg_rust_spec::spec::view_metadata::VersionBuilderError),
    /// create table builder
    #[error(transparent)]
    CreateTableBuilder(#[from] crate::catalog::create::CreateTableBuilderError),
    /// create view builder
    #[error(transparent)]
    CreateViewBuilder(#[from] crate::catalog::create::CreateViewBuilderError),
    /// create view builder
    #[error(transparent)]
    CreateMaterializedViewBuilder(
        #[from] crate::catalog::create::CreateMaterializedViewBuilderError,
    ),
}

impl From<Error> for ArrowError {
    fn from(value: Error) -> Self {
        ArrowError::from_external_error(Box::new(value))
    }
}
