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
//! # use iceberg_rust::arrow::write::write_parquet_partitioned;
//! # use iceberg_rust::table::Table;
//! # async fn example(table: &Table, batches: impl Stream<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send + 'static) {
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
use object_store::{buffered::BufWriter, ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::{fmt::Write, thread::available_parallelism};
use tokio::task::JoinSet;
use tracing::instrument;

use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use iceberg_rust_spec::{
    partition::BoundPartitionField,
    spec::{manifest::DataFile, schema::Schema, values::Value},
    table_metadata::{
        self, WRITE_DATA_PATH, WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED,
        WRITE_OBJECT_STORAGE_ENABLED, WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX,
        WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX, WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX,
        WRITE_PARQUET_COMPRESSION_CODEC, WRITE_PARQUET_COMPRESSION_LEVEL,
        WRITE_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX, WRITE_PARQUET_DICT_SIZE_BYTES,
        WRITE_PARQUET_PAGE_ROW_LIMIT, WRITE_PARQUET_PAGE_SIZE_BYTES, WRITE_PARQUET_PAGE_VERSION,
        WRITE_PARQUET_ROW_GROUP_SIZE_BYTES, WRITE_PARQUET_STATS_ENABLED_COLUMN_PREFIX,
        WRITE_TARGET_FILE_SIZE_BYTES,
    },
    util::strip_prefix,
};
use parquet::{
    arrow::AsyncArrowWriter,
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::{
        metadata::{KeyValue, ParquetMetaData},
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
    },
    schema::types::ColumnPath,
};
use uuid::Uuid;

use crate::{
    error::Error,
    file_format::parquet::{parquet_to_datafile, ICEBERG_ESTIMATE_INT64_DISTINCT_COUNT_META_KEY},
    object_store::Bucket,
    table::Table,
};

use super::partition::partition_record_batch;

/// Target size of a written data file, per the spec's default for
/// `write.target-file-size-bytes`.
const DEFAULT_TARGET_FILE_SIZE_BYTES: usize = 512 * 1024 * 1024;

/// The on-disk size at which a data file is rolled.
///
/// A zero or unparsable value falls back to the default; a zero target would
/// roll a new file for every batch.
fn target_file_size(table_properties: &HashMap<String, String>) -> usize {
    table_properties
        .get(WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TARGET_FILE_SIZE_BYTES)
}

/// Zstd level used when the table names zstd without a level, and the level
/// the writer falls back to when a table says nothing about compression.
const DEFAULT_ZSTD_LEVEL_VALUE: i32 = 1;

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
    let schema = Arc::new(metadata.current_schema().map_err(Error::from)?.clone());
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

    let partition_fields = &metadata.current_partition_fields().map_err(Error::from)?;

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
            &metadata.properties,
        )
        .await?;
        Ok(files)
    } else {
        let table_properties = Arc::new(metadata.properties.clone());
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
                        let table_properties = table_properties.clone();
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
                                &table_properties,
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
    table_properties: &HashMap<String, String>,
) -> Result<Vec<DataFile>, ArrowError> {
    let bucket = Bucket::from_path(data_location)?;
    let (mut writer_sender, writer_reciever): (ArrowSender, ArrowReciever) = channel(0);
    let table_properties_owned = Arc::new(table_properties.clone());

    // Create initial writer
    let initial_writer = create_arrow_writer(
        data_location,
        partition_path.clone(),
        arrow_schema,
        object_store.clone(),
        table_properties,
    )
    .await?;

    let target_file_size = target_file_size(table_properties);

    // Structure to hold writer state
    struct WriterState {
        writer: (String, AsyncArrowWriter<BufWriter>),
        rows_written: usize,
    }

    let final_state = batches
        .try_fold(
            WriterState {
                writer: initial_writer,
                rows_written: 0,
            },
            |mut state, batch| {
                let object_store = object_store.clone();
                let data_location = data_location.to_owned();
                let partition_path = partition_path.clone();
                let arrow_schema = arrow_schema.clone();
                let mut writer_sender = writer_sender.clone();
                let table_properties = table_properties_owned.clone();

                async move {
                    // Roll on the file's real on-disk size: what the writer has
                    // flushed plus the row group it still holds. The check runs
                    // before writing, so every file receives at least one batch
                    // and no empty file is ever emitted.
                    let file_size =
                        state.writer.1.bytes_written() + state.writer.1.in_progress_size();

                    if file_size >= target_file_size {
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
                            &table_properties,
                        )
                        .await?;

                        state.writer = new_writer;
                    }

                    state.rows_written += batch.num_rows();
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

    if final_state.rows_written == 0 {
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
                    table_properties,
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
    table_properties: &HashMap<String, String>,
) -> Result<(String, AsyncArrowWriter<BufWriter>), ArrowError> {
    let parquet_path = generate_file_path(data_location, partition_path);

    let writer = BufWriter::new(object_store.clone(), parquet_path.clone().into());

    let estimate_distinct_count = table_properties
        .get(WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED)
        .is_some_and(|x| x == "true");

    let mut props_builder = WriterProperties::builder().set_compression(Compression::ZSTD(
        ZstdLevel::try_new(DEFAULT_ZSTD_LEVEL_VALUE)?,
    ));
    props_builder = apply_writer_properties(props_builder, table_properties);
    props_builder = apply_bloom_filter_properties(props_builder, table_properties);
    props_builder = apply_column_write_properties(props_builder, table_properties);
    if estimate_distinct_count {
        props_builder = props_builder.set_key_value_metadata(Some(vec![KeyValue::new(
            ICEBERG_ESTIMATE_INT64_DISTINCT_COUNT_META_KEY.to_owned(),
            "true".to_owned(),
        )]));
    }

    Ok((
        parquet_path,
        AsyncArrowWriter::try_new(
            writer,
            Arc::new(schema.clone()),
            Some(props_builder.build()),
        )?,
    ))
}

/// Applies per-column bloom-filter table properties to the writer builder.
///
/// Honors the standard Iceberg properties
/// `write.parquet.bloom-filter-enabled.column.<name>` = `true`/`false`,
/// `write.parquet.bloom-filter-fpp.column.<name>` = a false-positive
/// probability in `(0, 1)` and `write.parquet.bloom-filter-ndv.column.<name>`
/// = an expected distinct-value count > 0. Unrelated properties are ignored.
fn apply_bloom_filter_properties(
    mut props_builder: parquet::file::properties::WriterPropertiesBuilder,
    table_properties: &HashMap<String, String>,
) -> parquet::file::properties::WriterPropertiesBuilder {
    for (key, value) in table_properties {
        if let Some(column) = key.strip_prefix(WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX) {
            let enabled = value.eq_ignore_ascii_case("true");
            props_builder =
                props_builder.set_column_bloom_filter_enabled(column_path(column), enabled);
        } else if let Some(column) = key.strip_prefix(WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX)
        {
            // A filter is sized from its target false-positive rate, so an
            // out-of-range value would produce a nonsensical filter. Ignore it
            // and keep the default rather than fail the write.
            if let Some(fpp) = value
                .parse::<f64>()
                .ok()
                .filter(|fpp| *fpp > 0.0 && *fpp < 1.0)
            {
                props_builder = props_builder.set_column_bloom_filter_fpp(column_path(column), fpp);
            }
        } else if let Some(column) = key.strip_prefix(WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX)
        {
            // A zero or negative ndv can't size a filter; ignore it and keep
            // the default rather than fail the write.
            if let Some(ndv) = value.parse::<u64>().ok().filter(|ndv| *ndv > 0) {
                props_builder = props_builder.set_column_bloom_filter_ndv(column_path(column), ndv);
            }
        }
    }
    props_builder
}

/// Parquet addresses nested columns by path parts; a dotted name passed as one
/// string would be treated as a single segment and silently never match.
fn column_path(column: &str) -> ColumnPath {
    ColumnPath::from(column.split('.').map(String::from).collect::<Vec<String>>())
}

/// Applies per-column Parquet write-behavior table properties to the writer.
///
/// Honors `write.parquet.stats-enabled.column.<name>` = `true`/`false`,
/// controlling whether Parquet writes column statistics into the file
/// footer for that column (distinct from `write.metadata.metrics.*`, which
/// controls the truncated stats recorded in the Iceberg manifest), and
/// `write.parquet.dict-encoding-enabled.column.<name>` = `true`/`false`,
/// controlling whether that column uses dictionary encoding.
fn apply_column_write_properties(
    mut props_builder: parquet::file::properties::WriterPropertiesBuilder,
    table_properties: &HashMap<String, String>,
) -> parquet::file::properties::WriterPropertiesBuilder {
    for (key, value) in table_properties {
        if let Some(column) = key.strip_prefix(WRITE_PARQUET_STATS_ENABLED_COLUMN_PREFIX) {
            let enabled = if value.eq_ignore_ascii_case("true") {
                EnabledStatistics::Page
            } else {
                EnabledStatistics::None
            };
            props_builder =
                props_builder.set_column_statistics_enabled(column_path(column), enabled);
        } else if let Some(column) =
            key.strip_prefix(WRITE_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX)
        {
            let enabled = value.eq_ignore_ascii_case("true");
            props_builder =
                props_builder.set_column_dictionary_enabled(column_path(column), enabled);
        }
    }
    props_builder
}

/// Applies the file-layout and compression table properties to the writer.
///
/// Honors `write.parquet.compression-codec`,
/// `write.parquet.compression-level`, `write.parquet.page-size-bytes`,
/// `write.parquet.page-row-limit`, `write.parquet.dict-size-bytes`,
/// `write.parquet.row-group-size-bytes` and `write.parquet.page-version`. An
/// unparsable or unknown value is ignored, so a bad property degrades to the
/// default instead of failing the write.
fn apply_writer_properties(
    mut props_builder: parquet::file::properties::WriterPropertiesBuilder,
    table_properties: &HashMap<String, String>,
) -> parquet::file::properties::WriterPropertiesBuilder {
    let level = table_properties
        .get(WRITE_PARQUET_COMPRESSION_LEVEL)
        .and_then(|value| value.parse::<u32>().ok());

    if let Some(codec) = table_properties
        .get(WRITE_PARQUET_COMPRESSION_CODEC)
        .and_then(|codec| compression_from(codec, level))
    {
        props_builder = props_builder.set_compression(codec);
    }

    if let Some(bytes) = parse_positive(table_properties, WRITE_PARQUET_PAGE_SIZE_BYTES) {
        props_builder = props_builder.set_data_page_size_limit(bytes);
    }
    if let Some(rows) = parse_positive(table_properties, WRITE_PARQUET_PAGE_ROW_LIMIT) {
        props_builder = props_builder.set_data_page_row_count_limit(rows);
    }
    if let Some(bytes) = parse_positive(table_properties, WRITE_PARQUET_DICT_SIZE_BYTES) {
        props_builder = props_builder.set_dictionary_page_size_limit(bytes);
    }
    if let Some(bytes) = parse_positive(table_properties, WRITE_PARQUET_ROW_GROUP_SIZE_BYTES) {
        props_builder = props_builder.set_max_row_group_bytes(Some(bytes));
    }
    if let Some(version) = table_properties
        .get(WRITE_PARQUET_PAGE_VERSION)
        .and_then(|version| writer_version_from(version))
    {
        props_builder = props_builder.set_writer_version(version);
    }

    props_builder
}

/// Resolve an Iceberg page-version name to a Parquet writer version.
///
/// Returns `None` for anything other than `v1`/`v2` so the caller keeps
/// whatever the builder had, matching the reference implementation's default
/// of `v1`.
fn writer_version_from(version: &str) -> Option<WriterVersion> {
    match version.trim().to_ascii_lowercase().as_str() {
        "v1" => Some(WriterVersion::PARQUET_1_0),
        "v2" => Some(WriterVersion::PARQUET_2_0),
        _ => None,
    }
}

fn parse_positive(table_properties: &HashMap<String, String>, key: &str) -> Option<usize> {
    table_properties
        .get(key)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

/// Resolve an Iceberg codec name, with its optional level, to a Parquet codec.
///
/// Returns `None` for an unknown codec so the caller keeps its default. A level
/// the codec rejects falls back to that codec's default level rather than
/// dropping the codec entirely.
fn compression_from(codec: &str, level: Option<u32>) -> Option<Compression> {
    match codec.trim().to_ascii_lowercase().as_str() {
        "uncompressed" | "none" => Some(Compression::UNCOMPRESSED),
        "snappy" => Some(Compression::SNAPPY),
        "lz4" => Some(Compression::LZ4),
        "lz4_raw" => Some(Compression::LZ4_RAW),
        "gzip" => Some(Compression::GZIP(
            level
                .and_then(|level| GzipLevel::try_new(level).ok())
                .unwrap_or_default(),
        )),
        "brotli" => Some(Compression::BROTLI(
            level
                .and_then(|level| BrotliLevel::try_new(level).ok())
                .unwrap_or_default(),
        )),
        "zstd" => Some(Compression::ZSTD(
            level
                .and_then(|level| ZstdLevel::try_new(level as i32).ok())
                .unwrap_or_else(|| {
                    ZstdLevel::try_new(DEFAULT_ZSTD_LEVEL_VALUE).unwrap_or_default()
                }),
        )),
        _ => None,
    }
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

    let base = strip_prefix(data_location);
    let separator = if base.ends_with('/') || path.starts_with('/') {
        ""
    } else {
        "/"
    };
    base + separator + &path + &Uuid::now_v1(&rand).to_string() + ".parquet"
}

#[cfg(test)]
mod target_file_size_tests {
    use super::*;

    fn properties(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    /// A table that says nothing gets the spec's 512 MiB default.
    #[test]
    fn the_default_target_is_the_spec_default() {
        assert_eq!(
            target_file_size(&HashMap::new()),
            DEFAULT_TARGET_FILE_SIZE_BYTES
        );
        assert_eq!(DEFAULT_TARGET_FILE_SIZE_BYTES, 536_870_912);
    }

    #[test]
    fn an_explicit_target_is_honored() {
        assert_eq!(
            target_file_size(&properties(&[(WRITE_TARGET_FILE_SIZE_BYTES, "134217728")])),
            134_217_728
        );
    }

    /// A zero target would roll a new file for every batch, and an unparsable
    /// one says nothing; both fall back rather than break the write.
    #[test]
    fn unusable_targets_fall_back_to_the_default() {
        for value in ["0", "-1", "lots", ""] {
            assert_eq!(
                target_file_size(&properties(&[(WRITE_TARGET_FILE_SIZE_BYTES, value)])),
                DEFAULT_TARGET_FILE_SIZE_BYTES,
                "target {value:?} should have fallen back"
            );
        }
    }
}

#[cfg(test)]
mod writer_properties_tests {
    use super::*;

    fn build(properties: &[(&str, &str)]) -> parquet::file::properties::WriterProperties {
        let properties: HashMap<String, String> = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        apply_writer_properties(WriterProperties::builder(), &properties).build()
    }

    /// A table naming a codec must get it. The writer previously hardcoded
    /// zstd level 1 while table creation recorded `zstd`/`3` in the metadata,
    /// so the properties described a file that was never written.
    #[test]
    fn the_compression_codec_and_level_are_honored() {
        let props = build(&[
            (WRITE_PARQUET_COMPRESSION_CODEC, "zstd"),
            (WRITE_PARQUET_COMPRESSION_LEVEL, "9"),
        ]);
        assert_eq!(
            props.compression(&ColumnPath::from("any")),
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
        );

        assert_eq!(
            build(&[(WRITE_PARQUET_COMPRESSION_CODEC, "SNAPPY")])
                .compression(&ColumnPath::from("any")),
            Compression::SNAPPY
        );
        assert_eq!(
            build(&[(WRITE_PARQUET_COMPRESSION_CODEC, "uncompressed")])
                .compression(&ColumnPath::from("any")),
            Compression::UNCOMPRESSED
        );
    }

    /// A codec that takes no level must ignore one rather than be dropped.
    #[test]
    fn a_level_on_a_levelless_codec_is_ignored() {
        let props = build(&[
            (WRITE_PARQUET_COMPRESSION_CODEC, "snappy"),
            (WRITE_PARQUET_COMPRESSION_LEVEL, "9"),
        ]);
        assert_eq!(
            props.compression(&ColumnPath::from("any")),
            Compression::SNAPPY
        );
    }

    /// A bad property must degrade to the default, never fail the write.
    #[test]
    fn unusable_values_fall_back_to_defaults() {
        let default = WriterProperties::builder().build();

        // Unknown codec: keep whatever the builder had.
        let props = build(&[(WRITE_PARQUET_COMPRESSION_CODEC, "rot13")]);
        assert_eq!(
            props.compression(&ColumnPath::from("any")),
            default.compression(&ColumnPath::from("any"))
        );

        // Out-of-range zstd level: keep the codec, use its default level.
        let props = build(&[
            (WRITE_PARQUET_COMPRESSION_CODEC, "zstd"),
            (WRITE_PARQUET_COMPRESSION_LEVEL, "999"),
        ]);
        assert!(matches!(
            props.compression(&ColumnPath::from("any")),
            Compression::ZSTD(_)
        ));

        // Unparsable and zero sizes are ignored.
        for value in ["not-a-number", "0"] {
            let props = build(&[(WRITE_PARQUET_PAGE_SIZE_BYTES, value)]);
            assert_eq!(
                props.data_page_size_limit(),
                default.data_page_size_limit(),
                "page size {value:?} should have been ignored"
            );
        }
    }

    /// Page and dictionary sizing bound how finely a reader can skip within a
    /// file, and were not reachable at all before.
    #[test]
    fn page_and_dictionary_sizing_are_honored() {
        let props = build(&[
            (WRITE_PARQUET_PAGE_SIZE_BYTES, "65536"),
            (WRITE_PARQUET_PAGE_ROW_LIMIT, "5000"),
            (WRITE_PARQUET_DICT_SIZE_BYTES, "131072"),
        ]);

        assert_eq!(props.data_page_size_limit(), 65536);
        assert_eq!(props.data_page_row_count_limit(), 5000);
        assert_eq!(props.dictionary_page_size_limit(), 131072);
    }

    /// `set_max_row_group_bytes` flushes a row group by estimated encoded
    /// size rather than row count, unlike the row-count-only knob it
    /// complements.
    #[test]
    fn row_group_size_bytes_is_honored() {
        let props = build(&[(WRITE_PARQUET_ROW_GROUP_SIZE_BYTES, "268435456")]);
        assert_eq!(props.max_row_group_bytes(), Some(268435456));

        for value in ["not-a-number", "0"] {
            let props = build(&[(WRITE_PARQUET_ROW_GROUP_SIZE_BYTES, value)]);
            assert_eq!(
                props.max_row_group_bytes(),
                None,
                "row group size {value:?} should have been ignored"
            );
        }
    }

    /// `v2` unlocks DataPageV2 encoding; an unrecognized value must keep the
    /// builder's default (`v1`) rather than fail the write.
    #[test]
    fn page_version_is_honored() {
        assert_eq!(
            build(&[(WRITE_PARQUET_PAGE_VERSION, "v2")]).writer_version(),
            WriterVersion::PARQUET_2_0
        );
        assert_eq!(
            build(&[(WRITE_PARQUET_PAGE_VERSION, "V1")]).writer_version(),
            WriterVersion::PARQUET_1_0
        );

        let default = WriterProperties::builder().build();
        assert_eq!(
            build(&[(WRITE_PARQUET_PAGE_VERSION, "v3")]).writer_version(),
            default.writer_version(),
            "an unrecognized page version should have been ignored"
        );
    }

    /// A bloom filter is sized from its target false-positive rate, so a
    /// high-cardinality column such as `trace_id` needs its own.
    #[test]
    fn per_column_bloom_filter_fpp_is_honored() {
        let properties: HashMap<String, String> = HashMap::from([
            (
                format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}trace_id"),
                "true".to_string(),
            ),
            (
                format!("{WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX}trace_id"),
                "0.001".to_string(),
            ),
        ]);
        let props = apply_bloom_filter_properties(WriterProperties::builder(), &properties).build();

        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("trace_id")),
            Some(&parquet::file::properties::BloomFilterProperties {
                fpp: 0.001,
                ndv: parquet::file::properties::DEFAULT_BLOOM_FILTER_NDV,
            })
        );
    }

    /// The fpp alone assumes the default cardinality; a high-cardinality
    /// column such as `trace_id` needs its real ndv to size the filter
    /// correctly.
    #[test]
    fn per_column_bloom_filter_ndv_is_honored() {
        let properties: HashMap<String, String> = HashMap::from([
            (
                format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}trace_id"),
                "true".to_string(),
            ),
            (
                format!("{WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX}trace_id"),
                "1000000".to_string(),
            ),
        ]);
        let props = apply_bloom_filter_properties(WriterProperties::builder(), &properties).build();

        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("trace_id")),
            Some(&parquet::file::properties::BloomFilterProperties {
                fpp: parquet::file::properties::DEFAULT_BLOOM_FILTER_FPP,
                ndv: 1_000_000,
            })
        );
    }

    /// A zero or unparsable ndv can't size a filter; it must be ignored
    /// rather than produce a nonsensical one.
    #[test]
    fn an_invalid_ndv_is_ignored() {
        for value in ["0", "-5", "many"] {
            let properties: HashMap<String, String> = HashMap::from([
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}trace_id"),
                    "true".to_string(),
                ),
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX}trace_id"),
                    value.to_string(),
                ),
            ]);
            let props =
                apply_bloom_filter_properties(WriterProperties::builder(), &properties).build();

            let ndv = props
                .bloom_filter_properties(&ColumnPath::from("trace_id"))
                .map(|properties| properties.ndv);
            assert_eq!(
                ndv,
                Some(parquet::file::properties::DEFAULT_BLOOM_FILTER_NDV),
                "ndv {value:?} should have been ignored"
            );
        }
    }

    /// An fpp outside `(0, 1)` cannot size a filter; it must be ignored rather
    /// than produce a nonsensical one.
    #[test]
    fn an_out_of_range_fpp_is_ignored() {
        for value in ["0", "1", "-0.5", "1.5", "many"] {
            let properties: HashMap<String, String> = HashMap::from([
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}trace_id"),
                    "true".to_string(),
                ),
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX}trace_id"),
                    value.to_string(),
                ),
            ]);
            let props =
                apply_bloom_filter_properties(WriterProperties::builder(), &properties).build();

            let fpp = props
                .bloom_filter_properties(&ColumnPath::from("trace_id"))
                .map(|properties| properties.fpp);
            assert_eq!(
                fpp,
                Some(parquet::file::properties::DEFAULT_BLOOM_FILTER_FPP),
                "fpp {value:?} should have been ignored"
            );
        }
    }

    /// Distinct from `write.metadata.metrics.*`: this toggles whether
    /// Parquet itself writes column stats into the file footer, defaulting
    /// to on (`EnabledStatistics::Page`).
    #[test]
    fn column_statistics_enabled_is_honored() {
        let properties: HashMap<String, String> = HashMap::from([(
            format!("{WRITE_PARQUET_STATS_ENABLED_COLUMN_PREFIX}body"),
            "false".to_string(),
        )]);
        let props = apply_column_write_properties(WriterProperties::builder(), &properties).build();

        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("body")),
            EnabledStatistics::None
        );
        // An untouched column keeps the builder's default.
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("other")),
            EnabledStatistics::Page
        );

        let properties: HashMap<String, String> = HashMap::from([(
            format!("{WRITE_PARQUET_STATS_ENABLED_COLUMN_PREFIX}body"),
            "true".to_string(),
        )]);
        let props = apply_column_write_properties(WriterProperties::builder(), &properties).build();
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("body")),
            EnabledStatistics::Page
        );
    }

    /// Turning dictionary encoding off is useful for a column whose values
    /// are rarely repeated, where a dictionary just adds overhead.
    #[test]
    fn column_dictionary_encoding_is_honored() {
        let properties: HashMap<String, String> = HashMap::from([(
            format!("{WRITE_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX}trace_id"),
            "false".to_string(),
        )]);
        let props = apply_column_write_properties(WriterProperties::builder(), &properties).build();

        assert!(!props.dictionary_enabled(&ColumnPath::from("trace_id")));
        // An untouched column keeps the builder's default (enabled).
        assert!(props.dictionary_enabled(&ColumnPath::from("other")));

        let properties: HashMap<String, String> = HashMap::from([(
            format!("{WRITE_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX}trace_id"),
            "true".to_string(),
        )]);
        let props = apply_column_write_properties(WriterProperties::builder(), &properties).build();
        assert!(props.dictionary_enabled(&ColumnPath::from("trace_id")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_filter_properties_apply_per_column() {
        let table_properties = HashMap::from([
            (
                "write.parquet.bloom-filter-enabled.column.label_env".to_string(),
                "true".to_string(),
            ),
            (
                "write.parquet.bloom-filter-enabled.column.body".to_string(),
                "false".to_string(),
            ),
            ("write.data.path".to_string(), "s3://x".to_string()),
        ]);
        let props =
            apply_bloom_filter_properties(WriterProperties::builder(), &table_properties).build();
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("label_env"))
            .is_some());
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("body"))
            .is_none());
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("other"))
            .is_none());
    }

    #[test]
    fn bloom_filter_properties_apply_to_nested_columns() {
        let table_properties = HashMap::from([(
            "write.parquet.bloom-filter-enabled.column.my_struct.label_env".to_string(),
            "true".to_string(),
        )]);
        let props =
            apply_bloom_filter_properties(WriterProperties::builder(), &table_properties).build();
        // Parquet addresses nested columns by path parts, not by the dotted string.
        assert!(props
            .bloom_filter_properties(&ColumnPath::from(vec![
                "my_struct".to_string(),
                "label_env".to_string()
            ]))
            .is_some());
    }

    #[test]
    fn bloom_filter_property_values_are_case_insensitive() {
        let table_properties = HashMap::from([
            (
                "write.parquet.bloom-filter-enabled.column.label_env".to_string(),
                "TRUE".to_string(),
            ),
            (
                "write.parquet.bloom-filter-enabled.column.body".to_string(),
                "False".to_string(),
            ),
        ]);
        let props =
            apply_bloom_filter_properties(WriterProperties::builder(), &table_properties).build();
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("label_env"))
            .is_some());
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("body"))
            .is_none());
    }

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
            initial_default: None,
            write_default: None,
        };
        let partfield = PartitionField::new(1, 1001, "month", Transform::Month);
        let partition_fields = vec![BoundPartitionField::new(&partfield, &field)];
        let partition_values = vec![Value::Int(10)];

        let result = super::generate_partition_path(&partition_fields, &partition_values);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "month=10/");
    }
}
