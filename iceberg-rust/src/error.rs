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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_format_display_includes_argument() {
        let err = Error::InvalidFormat("schema field".to_string());
        assert_eq!(
            err.to_string(),
            "schema field doesn't have the right format"
        );
    }

    #[test]
    fn test_type_display_includes_both_arguments() {
        let err = Error::Type("\"x\"".to_string(), "int".to_string());
        assert_eq!(err.to_string(), "Value \"x\" doesn't have the int type.");
    }

    #[test]
    fn test_schema_display_includes_column_and_schema_name() {
        let err = Error::Schema("user_id".to_string(), "events".to_string());
        assert_eq!(err.to_string(), "Column user_id not in schema events.");
    }

    #[test]
    fn test_conversion_display_uses_to_from_phrasing() {
        let err = Error::Conversion("json".to_string(), "bytes".to_string());
        assert_eq!(err.to_string(), "Failed to convert json to bytes.");
    }

    #[test]
    fn test_not_found_and_not_supported_display() {
        assert_eq!(
            Error::NotFound("table".to_string()).to_string(),
            "table not found.",
        );
        assert_eq!(
            Error::NotSupported("eq deletes".to_string()).to_string(),
            "Feature eq deletes is not supported.",
        );
        assert_eq!(
            Error::CatalogNotFound.to_string(),
            "Entity not found in catalog"
        );
    }

    #[test]
    fn test_decompress_display_includes_message() {
        let err = Error::Decompress("CRC mismatch".to_string());
        assert_eq!(
            err.to_string(),
            "Failed to decompress gzip data: CRC mismatch"
        );
    }

    #[test]
    fn test_from_iceberg_rust_spec_lands_in_iceberg_variant() {
        let inner = iceberg_rust_spec::error::Error::NotFound("x".to_string());
        let err: Error = inner.into();
        assert!(matches!(err, Error::Iceberg(_)));
    }

    #[test]
    fn test_from_arrow_lands_in_arrow_variant() {
        let arrow_err = ArrowError::SchemaError("bad".to_string());
        let err: Error = arrow_err.into();
        assert!(matches!(err, Error::Arrow(_)));
    }

    #[test]
    fn test_from_serde_json_lands_in_jsonserde_variant() {
        let serde_err = serde_json::from_str::<i32>("not-json").unwrap_err();
        let err: Error = serde_err.into();
        assert!(matches!(err, Error::JSONSerde(_)));
    }

    #[test]
    fn test_from_parse_int_lands_in_parseint_variant() {
        let parse_err = "12abc".parse::<i32>().unwrap_err();
        let err: Error = parse_err.into();
        assert!(matches!(err, Error::ParseInt(_)));
    }

    #[test]
    fn test_from_apache_avro_boxes_into_avro_variant() {
        let avro_err = apache_avro::Schema::parse_str("{ not json }").unwrap_err();
        let err: Error = avro_err.into();
        assert!(matches!(err, Error::Avro(_)));
    }

    #[test]
    fn test_iceberg_rust_error_round_trip_via_arrow_external_error() {
        // `From<Error> for ArrowError` wraps the iceberg error as an
        // external error; the resulting ArrowError must carry our message
        // through Display.
        let err = Error::Schema("colA".to_string(), "schemaA".to_string());
        let displayed = err.to_string();
        let arrow_err: ArrowError = err.into();
        let arrow_displayed = arrow_err.to_string();
        assert!(
            arrow_displayed.contains(&displayed),
            "arrow message `{arrow_displayed}` should include `{displayed}`",
        );
    }
}
