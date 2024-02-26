/*!
 * Functions to write arrow record batches to an iceberg table
*/

use futures::{
    channel::mpsc::{channel, unbounded, Receiver, Sender},
    lock::Mutex,
    stream, SinkExt, StreamExt, TryStreamExt,
};
use object_store::ObjectStore;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::AsyncWrite;

use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use iceberg_rust_spec::{
    spec::{
        manifest::DataFile, partition::PartitionSpec, schema::Schema,
        table_metadata::TableMetadata, values::Value,
    },
    util::strip_prefix,
};
use parquet::{
    arrow::AsyncArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use uuid::Uuid;

use crate::{
    catalog::bucket::parse_bucket, error::Error, file_format::parquet::parquet_to_datafile,
};

use super::partition::partition_record_batches;

const MAX_PARQUET_SIZE: usize = 512_000_000;

/// Partitions arrow record batches and writes them to parquet files. Does not perform any operation on an iceberg table.
pub async fn write_parquet_partitioned(
    metadata: &TableMetadata,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
    branch: Option<&str>,
) -> Result<Vec<DataFile>, ArrowError> {
    let location = &metadata.location;
    let schema = metadata.current_schema(branch).map_err(Error::from)?;
    let partition_spec = metadata.default_partition_spec().map_err(Error::from)?;

    let streams = partition_record_batches(batches, partition_spec, schema).await?;

    let arrow_schema: Arc<ArrowSchema> =
        Arc::new((schema.fields()).try_into().map_err(Error::from)?);

    let (sender, reciever) = unbounded();

    stream::iter(streams.into_iter())
        .map(Ok::<_, ArrowError>)
        .try_for_each_concurrent(None, |(partition_values, batches)| {
            let arrow_schema = arrow_schema.clone();
            let object_store = object_store.clone();
            let mut sender = sender.clone();
            async move {
                let files = write_parquet_files(
                    location,
                    schema,
                    &arrow_schema,
                    partition_spec,
                    &partition_values,
                    batches,
                    object_store.clone(),
                )
                .await?;
                sender.send(files).await.map_err(Error::from)?;
                Ok(())
            }
        })
        .await?;

    sender.close_channel();

    Ok(reciever
        .fold(Vec::new(), |mut acc, x| async move {
            acc.extend(x);
            acc
        })
        .await)
}

type SendableAsyncArrowWriter = AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>;
type ArrowSender = Sender<(String, SendableAsyncArrowWriter)>;
type ArrowReciever = Receiver<(String, SendableAsyncArrowWriter)>;

/// Write arrow record batches to parquet files. Does not perform any operation on an iceberg table.
async fn write_parquet_files(
    location: &str,
    schema: &Schema,
    arrow_schema: &ArrowSchema,
    partition_spec: &PartitionSpec,
    partiton_values: &[Value],
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<DataFile>, ArrowError> {
    let bucket = parse_bucket(location)?;
    let partition_location =
        generate_partition_location(location, partition_spec, partiton_values)?;
    let current_writer = Arc::new(Mutex::new(
        create_arrow_writer(&partition_location, arrow_schema, object_store.clone()).await?,
    ));

    let (mut writer_sender, writer_reciever): (ArrowSender, ArrowReciever) = channel(32);

    let num_bytes = Arc::new(AtomicUsize::new(0));

    batches
        .try_for_each_concurrent(None, |batch| {
            let current_writer = current_writer.clone();
            let mut writer_sender = writer_sender.clone();
            let num_bytes = num_bytes.clone();
            let object_store = object_store.clone();
            let partition_location = &partition_location;
            async move {
                let mut current_writer = current_writer.lock().await;
                let current =
                    num_bytes.fetch_update(Ordering::Release, Ordering::Acquire, |current| {
                        if current > MAX_PARQUET_SIZE {
                            Some(0)
                        } else {
                            None
                        }
                    });
                if current.is_ok() {
                    let finished_writer = std::mem::replace(
                        &mut *current_writer,
                        create_arrow_writer(partition_location, arrow_schema, object_store)
                            .await
                            .unwrap(),
                    );
                    writer_sender
                        .try_send(finished_writer)
                        .map_err(|err| ArrowError::ComputeError(err.to_string()))?;
                }
                current_writer.1.write(&batch).await?;
                let batch_size = record_batch_size(&batch);
                num_bytes.fetch_add(batch_size, Ordering::AcqRel);
                Ok(())
            }
        })
        .await?;

    let last = Arc::try_unwrap(current_writer).unwrap().into_inner();

    writer_sender.try_send(last).unwrap();

    writer_sender.close_channel();

    writer_reciever
        .then(|writer| {
            let object_store = object_store.clone();
            let bucket = bucket.to_string();
            async move {
                let metadata = writer.1.close().await?;
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
                    partition_spec.fields(),
                )?)
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

#[inline]
fn generate_partition_location(
    location: &str,
    partition_spec: &PartitionSpec,
    partiton_values: &[Value],
) -> Result<String, ArrowError> {
    let partition_location = strip_prefix(location)
        + "/data/"
        + &partition_spec
            .fields()
            .iter()
            .zip(partiton_values.iter())
            .map(|(spec, value)| {
                let name = spec.name().clone();
                Ok(name + "=" + &value.to_string() + "/")
            })
            .collect::<Result<String, ArrowError>>()?;
    Ok(partition_location)
}

async fn create_arrow_writer(
    partition_location: &str,
    schema: &arrow::datatypes::Schema,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>), ArrowError> {
    let mut rand = [0u8; 6];
    getrandom::getrandom(&mut rand)
        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
        .unwrap();

    let parquet_path =
        partition_location.to_string() + &Uuid::now_v1(&rand).to_string() + ".parquet";

    let (_, writer) = object_store
        .put_multipart(&parquet_path.clone().into())
        .await
        .map_err(|err| ArrowError::from_external_error(err.into()))?;

    Ok((
        parquet_path,
        AsyncArrowWriter::try_new(
            writer,
            Arc::new(schema.clone()),
            1024,
            Some(
                WriterProperties::builder()
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
                    .build(),
            ),
        )?,
    ))
}

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
    use crate::spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        values::Value,
    };

    #[test]
    fn test_generate_partiton_location_success() {
        let location = "s3://bucket/table";
        let partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_partition_field(PartitionField::new(1, 1001, "month", Transform::Month))
            .build()
            .unwrap();
        let partiton_values = vec![Value::Int(10)];

        let result =
            super::generate_partition_location(location, &partition_spec, &partiton_values);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "/table/data/month=10/");
    }
}
