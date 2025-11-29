//! Arrow writing module for converting Arrow record batches to Iceberg data files.
//!
//! This module provides functionality to:
//! - Write Arrow record batches to Parquet files
//! - Handle partitioned data writing
//! - Support equality delete files
//! - Manage file sizes and buffering
//!
//! The main entry points are:
//! - [`write_parquet_partitioned`]: Write regular data files
//! - [`write_equality_deletes_parquet_partitioned`]: Write equality delete files
//!
//! The module handles:
//! - Automatic file size management and splitting
//! - Parquet compression and encoding
//! - Partition path generation
//! - Object store integration
//! - Metadata collection for written files
//!
//! # Example
//!
//! ```no_run
//! # use arrow::record_batch::RecordBatch;
//! # use futures::Stream;
//! # use iceberg_rust::table::Table;
//! # async fn example(table: &Table, batches: impl Stream<Item = Result<RecordBatch, arrow::error::ArrowError>>) {
//! let data_files = write_parquet_partitioned(
//!     table,
//!     batches,
//!     None // no specific branch
//! ).await.unwrap();
//! # }
//! ```

use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use lru::LruCache;
use object_store::{buffered::BufWriter, ObjectStore};
use std::sync::Arc;
use std::{fmt::Write, thread::available_parallelism};
use tokio::task::JoinSet;
use tracing::instrument;

use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use iceberg_rust_spec::{
    partition::BoundPartitionField,
    spec::{manifest::DataFile, schema::Schema, values::Value},
    table_metadata::{self, WRITE_DATA_PATH, WRITE_OBJECT_STORAGE_ENABLED},
    util::strip_prefix,
};
use parquet::{
    arrow::AsyncArrowWriter,
    basic::{Compression, ZstdLevel},
    file::{metadata::ParquetMetaData, properties::WriterProperties},
};
use uuid::Uuid;

use crate::{
    error::Error, file_format::parquet::parquet_to_datafile, object_store::Bucket, table::Table,
};

use super::partition::partition_record_batch;

const MAX_PARQUET_SIZE: usize = 512_000_000;
const COMPRESSION_FACTOR: usize = 200;

#[instrument(skip(table, batches), fields(table_name = %table.identifier().name()))]
/// Writes Arrow record batches as partitioned Parquet files.
///
/// This function writes Arrow record batches to Parquet files, partitioning them according
/// to the table's partition spec.
///
/// # Arguments
/// * `table` - The Iceberg table to write data for
/// * `batches` - Stream of Arrow record batches to write
/// * `branch` - Optional branch name to write to
///
/// # Returns
/// * `Result<Vec<DataFile>, ArrowError>` - List of metadata for the written data files
///
/// # Errors
/// Returns an error if:
/// * The table metadata cannot be accessed
/// * The schema projection fails
/// * The object store operations fail
/// * The Parquet writing fails
/// * The partition path generation fails
pub async fn write_parquet_partitioned(
    table: &Table,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    branch: Option<&str>,
) -> Result<Vec<DataFile>, ArrowError> {
    store_parquet_partitioned(table, batches, branch, None).await
}

#[instrument(skip(table, batches), fields(table_name = %table.identifier().name(), equality_ids = ?equality_ids))]
/// Writes equality delete records as partitioned Parquet files.
///
/// This function writes Arrow record batches containing equality delete records to Parquet files,
/// partitioning them according to the table's partition spec.
///
/// # Arguments
/// * `table` - The Iceberg table to write delete records for
/// * `batches` - Stream of Arrow record batches containing the delete records
/// * `branch` - Optional branch name to write to
/// * `equality_ids` - Field IDs that define equality deletion
///
/// # Returns
/// * `Result<Vec<DataFile>, ArrowError>` - List of metadata for the written delete files
///
/// # Errors
/// Returns an error if:
/// * The table metadata cannot be accessed
/// * The schema projection fails
/// * The object store operations fail
/// * The Parquet writing fails
/// * The partition path generation fails
pub async fn write_equality_deletes_parquet_partitioned(
    table: &Table,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    branch: Option<&str>,
    equality_ids: &[i32],
) -> Result<Vec<DataFile>, ArrowError> {
    store_parquet_partitioned(table, batches, branch, Some(equality_ids)).await
}

#[instrument(skip(table, batches), fields(table_name = %table.identifier().name(), equality_ids = ?equality_ids))]
/// Stores Arrow record batches as partitioned Parquet files.
///
/// This is an internal function that handles the core storage logic for both regular data files
/// and equality delete files.
///
/// # Arguments
/// * `table` - The Iceberg table to store data for
/// * `batches` - Stream of Arrow record batches to write
/// * `branch` - Optional branch name to write to
/// * `equality_ids` - Optional list of field IDs for equality deletes
///
/// # Returns
/// * `Result<Vec<DataFile>, ArrowError>` - List of metadata for the written data files
///
/// # Errors
/// Returns an error if:
/// * The table metadata cannot be accessed
/// * The schema projection fails
/// * The object store operations fail
/// * The Parquet writing fails
/// * The partition path generation fails
async fn store_parquet_partitioned(
    table: &Table,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    branch: Option<&str>,
    equality_ids: Option<&[i32]>,
) -> Result<Vec<DataFile>, ArrowError> {
    let metadata = table.metadata();
    let object_store = table.object_store();
    let schema = Arc::new(
        metadata
            .current_schema(branch)
            .map_err(Error::from)?
            .clone(),
    );
    // project the schema on to the equality_ids for equality deletes
    let schema = if let Some(equality_ids) = equality_ids {
        Arc::new(schema.project(equality_ids))
    } else {
        schema
    };

    let partition_spec = Arc::new(
        metadata
            .default_partition_spec()
            .map_err(Error::from)?
            .clone(),
    );

    let partition_fields = &metadata
        .current_partition_fields(branch)
        .map_err(Error::from)?;

    let data_location = &metadata
        .properties
        .get(WRITE_DATA_PATH)
        .map(ToOwned::to_owned)
        .unwrap_or(metadata.location.clone() + "/data/");

    let arrow_schema: Arc<ArrowSchema> =
        Arc::new((schema.fields()).try_into().map_err(Error::from)?);

    if partition_fields.is_empty() {
        let partition_path = if metadata
            .properties
            .get(WRITE_OBJECT_STORAGE_ENABLED)
            .is_some_and(|x| x == "true")
        {
            Some("".to_owned())
        } else {
            None
        };
        let files = write_parquet_files(
            data_location,
            &schema,
            &arrow_schema,
            partition_fields,
            partition_path,
            batches,
            object_store.clone(),
            equality_ids,
        )
        .await?;
        Ok(files)
    } else {
        let mut senders: LruCache<Vec<Value>, Sender<Result<RecordBatch, ArrowError>>> =
            LruCache::unbounded();

        let mut set = JoinSet::new();
        // let receiver_handles = Vec::new();

        let mut batches = Box::pin(batches);

        while let Some(batch) = batches.next().await {
            // Limit the number of concurrent senders
            if senders.len() > available_parallelism().unwrap().get() {
                if let Some((_, mut sender)) = senders.pop_lru() {
                    sender.close_channel();
                }
            }

            for result in partition_record_batch(&batch?, partition_fields)? {
                let (partition_values, batch) = result?;

                if let Some(sender) = senders.get_mut(&partition_values) {
                    sender.send(Ok(batch)).await.unwrap();
                } else {
                    let (mut sender, reciever) = channel(1);
                    sender.send(Ok(batch)).await.unwrap();
                    senders.push(partition_values.clone(), sender);
                    set.spawn({
                        let arrow_schema = arrow_schema.clone();
                        let object_store = object_store.clone();
                        let data_location = data_location.clone();
                        let schema = schema.clone();
                        let partition_spec = partition_spec.clone();
                        let equality_ids = equality_ids.map(Vec::from);
                        let partition_path = if metadata
                            .properties
                            .get(WRITE_OBJECT_STORAGE_ENABLED)
                            .is_some_and(|x| x == "true")
                        {
                            None
                        } else {
                            Some(generate_partition_path(
                                partition_fields,
                                &partition_values,
                            )?)
                        };
                        async move {
                            let partition_fields =
                                table_metadata::partition_fields(&partition_spec, &schema)
                                    .map_err(Error::from)?;
                            let files = write_parquet_files(
                                &data_location,
                                &schema,
                                &arrow_schema,
                                &partition_fields,
                                partition_path,
                                reciever,
                                object_store.clone(),
                                equality_ids.as_deref(),
                            )
                            .await?;
                            Ok::<_, Error>(files)
                        }
                    });
                };
            }
        }

        while let Some((_, mut sender)) = senders.pop_lru() {
            sender.close_channel();
        }

        let mut files = Vec::new();

        while let Some(handle) = set.join_next().await {
            files.extend(handle.map_err(Error::from)??);
        }

        Ok(files)
    }
}

type ArrowSender = Sender<(String, ParquetMetaData)>;
type ArrowReciever = Receiver<(String, ParquetMetaData)>;

#[instrument(skip(batches, object_store), fields(data_location, equality_ids = ?equality_ids))]
/// Writes a stream of Arrow record batches to multiple Parquet files.
///
/// This internal function handles the low-level details of writing record batches to Parquet files,
/// managing file sizes, and collecting metadata.
///
/// # Arguments
/// * `data_location` - Base path where data files should be written
/// * `schema` - Iceberg schema for the data
/// * `arrow_schema` - Arrow schema for the record batches
/// * `partition_fields` - List of partition fields if data is partitioned
/// * `partition_path` - Optional partition path component
/// * `batches` - Stream of record batches to write
/// * `object_store` - Object store to write files to
/// * `equality_ids` - Optional list of field IDs for equality deletes
///
/// # Returns
/// * `Result<Vec<DataFile>, ArrowError>` - List of metadata for the written files
///
/// # Errors
/// Returns an error if:
/// * File creation fails
/// * Writing record batches fails
/// * Object store operations fail
/// * Metadata collection fails
#[allow(clippy::too_many_arguments)]
async fn write_parquet_files(
    data_location: &str,
    schema: &Schema,
    arrow_schema: &ArrowSchema,
    partition_fields: &[BoundPartitionField<'_>],
    partition_path: Option<String>,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
    equality_ids: Option<&[i32]>,
) -> Result<Vec<DataFile>, ArrowError> {
    let bucket = Bucket::from_path(data_location)?;
    let (mut writer_sender, writer_reciever): (ArrowSender, ArrowReciever) = channel(0);

    // Create initial writer
    let initial_writer = create_arrow_writer(
        data_location,
        partition_path.clone(),
        arrow_schema,
        object_store.clone(),
    )
    .await?;

    // Structure to hold writer state
    struct WriterState {
        writer: (String, AsyncArrowWriter<BufWriter>),
        bytes_written: usize,
    }

    let final_state = batches
        .try_fold(
            WriterState {
                writer: initial_writer,
                bytes_written: 0,
            },
            |mut state, batch| {
                let object_store = object_store.clone();
                let data_location = data_location.to_owned();
                let partition_path = partition_path.clone();
                let arrow_schema = arrow_schema.clone();
                let mut writer_sender = writer_sender.clone();

                async move {
                    let batch_size = record_batch_size(&batch);
                    let new_size = state.bytes_written + batch_size;

                    if new_size > COMPRESSION_FACTOR * MAX_PARQUET_SIZE {
                        // Send current writer to channel
                        let finished_writer = state.writer;
                        let file = finished_writer.1.close().await?;
                        writer_sender
                            .try_send((finished_writer.0, file))
                            .map_err(|err| ArrowError::ComputeError(err.to_string()))?;

                        // Create new writer
                        let new_writer = create_arrow_writer(
                            &data_location,
                            partition_path,
                            &arrow_schema,
                            object_store,
                        )
                        .await?;

                        state.writer = new_writer;
                        state.bytes_written = batch_size;
                    } else {
                        state.bytes_written = new_size;
                    }

                    state.writer.1.write(&batch).await?;
                    Ok(state)
                }
            },
        )
        .await?;

    // Handle the last writer
    let file = final_state.writer.1.close().await?;
    writer_sender
        .try_send((final_state.writer.0, file))
        .map_err(|err| ArrowError::ComputeError(err.to_string()))?;
    writer_sender.close_channel();

    if final_state.bytes_written == 0 {
        return Ok(Vec::new());
    }

    writer_reciever
        .then(|writer| {
            let object_store = object_store.clone();
            let bucket = bucket.to_string();
            async move {
                let metadata = writer.1;
                let size = object_store
                    .head(&writer.0.as_str().into())
                    .await
                    .map_err(|err| ArrowError::from_external_error(err.into()))?
                    .size;
                Ok(parquet_to_datafile(
                    &(bucket + &writer.0),
                    size,
                    &metadata,
                    schema,
                    partition_fields,
                    equality_ids,
                )?)
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

/// Generates a partition path string from partition fields and their values.
///
/// Creates a path string in the format "field1=value1/field2=value2/..." for each
/// partition field and its corresponding value.
///
/// # Arguments
/// * `partition_fields` - List of bound partition fields defining the partitioning
/// * `partition_values` - List of values for each partition field
///
/// # Returns
/// * `Result<String, ArrowError>` - The generated partition path string
///
/// # Errors
/// Returns an error if:
/// * The partition field name cannot be processed
/// * The partition value cannot be converted to a string
#[inline]
pub fn generate_partition_path(
    partition_fields: &[BoundPartitionField<'_>],
    partition_values: &[Value],
) -> Result<String, ArrowError> {
    partition_fields
        .iter()
        .zip(partition_values.iter())
        .map(|(field, value)| {
            let name = field.name().to_owned();
            Ok(name + "=" + &value.to_string() + "/")
        })
        .collect::<Result<String, ArrowError>>()
}

#[instrument(skip(schema, object_store), fields(data_location))]
/// Creates a new Arrow writer for writing record batches to a Parquet file.
///
/// This internal function creates a new buffered writer and configures it with
/// appropriate Parquet compression settings.
///
/// # Arguments
/// * `data_location` - Base path where data files should be written
/// * `partition_path` - Optional partition path component
/// * `schema` - Arrow schema for the record batches
/// * `object_store` - Object store to write files to
///
/// # Returns
/// * `Result<(String, AsyncArrowWriter<BufWriter>), ArrowError>` - The file path and configured writer
///
/// # Errors
/// Returns an error if:
/// * Random number generation fails
/// * The writer properties cannot be configured
/// * The Arrow writer cannot be created
async fn create_arrow_writer(
    data_location: &str,
    partition_path: Option<String>,
    schema: &arrow::datatypes::Schema,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, AsyncArrowWriter<BufWriter>), ArrowError> {
    let parquet_path = generate_file_path(data_location, partition_path);

    let writer = BufWriter::new(object_store.clone(), parquet_path.clone().into());

    Ok((
        parquet_path,
        AsyncArrowWriter::try_new(
            writer,
            Arc::new(schema.clone()),
            Some(
                WriterProperties::builder()
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
                    .build(),
            ),
        )?,
    ))
}

/// Generates a unique file path for a Parquet data file.
///
/// This function creates a unique file path by combining the data location, partition path,
/// and a UUID-based filename. If no partition path is provided, it generates a random
/// directory path using hex-encoded random bytes.
///
/// # Arguments
/// * `data_location` - Base directory where data files should be stored
/// * `partition_path` - Optional partition path component (e.g., "year=2024/month=01/")
///
/// # Returns
/// * `String` - Complete file path ending with ".parquet"
///
/// # File Path Structure
/// The generated path follows this pattern:
/// * With partition: `{data_location}/{partition_path}{uuid}.parquet`
/// * Without partition: `{data_location}/{random_hex}/{uuid}.parquet`
///
/// # Examples
/// ```
/// use iceberg_rust::arrow::write::generate_file_path;
///
/// // With partition path
/// let path1 = generate_file_path("/data", Some("year=2024/month=01/".to_string()));
/// // Result: "/data/year=2024/month=01/01234567-89ab-cdef-0123-456789abcdef.parquet"
///
/// // Without partition path (generates random directory)
/// let path2 = generate_file_path("/data", None);
/// // Result: "/data/a1b/01234567-89ab-cdef-0123-456789abcdef.parquet"
/// ```
///
/// # Implementation Details
/// * Uses cryptographically secure random bytes for UUID generation
/// * Creates a UUID v1 timestamp-based identifier for uniqueness
/// * Random directory names use 3 bytes of entropy (6 hex characters)
/// * Automatically strips path prefixes using `strip_prefix()`
pub fn generate_file_path(data_location: &str, partition_path: Option<String>) -> String {
    let mut rand = [0u8; 6];
    getrandom::fill(&mut rand)
        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
        .unwrap();

    let path = partition_path.unwrap_or_else(|| {
        rand[0..3]
            .iter()
            .fold(String::with_capacity(8), |mut acc, x| {
                write!(&mut acc, "{x:x}").unwrap();
                acc
            })
            + "/"
    });

    strip_prefix(data_location) + &path + &Uuid::now_v1(&rand).to_string() + ".parquet"
}

/// Calculates the approximate size in bytes of an Arrow record batch.
///
/// This function estimates the memory footprint of a record batch by multiplying
/// the total size of all fields by the number of rows.
///
/// # Arguments
/// * `batch` - The record batch to calculate size for
///
/// # Returns
/// * `usize` - Estimated size of the record batch in bytes
#[inline]
fn record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .schema()
        .fields()
        .iter()
        .fold(0, |acc, x| acc + x.size())
        * batch.num_rows()
}

#[cfg(test)]
mod tests {
    use iceberg_rust_spec::{
        partition::BoundPartitionField,
        types::{StructField, Type},
    };

    use crate::spec::{
        partition::{PartitionField, Transform},
        values::Value,
    };

    #[test]
    fn test_generate_partition_location_success() {
        let field = StructField {
            id: 0,
            name: "date".to_owned(),
            required: false,
            field_type: Type::Primitive(iceberg_rust_spec::types::PrimitiveType::Date),
            doc: None,
        };
        let partfield = PartitionField::new(1, 1001, "month", Transform::Month);
        let partition_fields = vec![BoundPartitionField::new(&partfield, &field)];
        let partition_values = vec![Value::Int(10)];

        let result = super::generate_partition_path(&partition_fields, &partition_values);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "month=10/");
    }
}
