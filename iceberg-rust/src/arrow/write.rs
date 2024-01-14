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
use iceberg_rust_spec::spec::{manifest::DataFile, partition::PartitionSpec, schema::Schema};
use parquet::arrow::AsyncArrowWriter;
use uuid::Uuid;

use crate::{error::Error, file_format::parquet::parquet_to_datafile};

use super::partition::partition_record_batches;

const MAX_PARQUET_SIZE: usize = 512_000_000;

/// Partitions arrow record batches and writes them to parquet files. Does not perform any operation on an iceberg table.
pub async fn write_parquet_partitioned(
    location: &str,
    schema: &Schema,
    partition_spec: &PartitionSpec,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<DataFile>, ArrowError> {
    let streams = partition_record_batches(batches, partition_spec, schema).await?;
    let arrow_schema: Arc<ArrowSchema> =
        Arc::new((&schema.fields).try_into().map_err(Error::from)?);
    let (sender, reciever) = unbounded();
    stream::iter(streams.into_iter())
        .map(Ok::<_, ArrowError>)
        .try_for_each_concurrent(None, |batches| {
            let arrow_schema = arrow_schema.clone();
            let object_store = object_store.clone();
            let mut sender = sender.clone();
            async move {
                let files = write_parquet_files(
                    location,
                    schema,
                    &arrow_schema,
                    partition_spec,
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
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<DataFile>, ArrowError> {
    let current_writer = Arc::new(Mutex::new(
        create_arrow_writer(location, arrow_schema, object_store.clone()).await?,
    ));

    let (mut writer_sender, writer_reciever): (ArrowSender, ArrowReciever) = channel(32);

    let num_bytes = Arc::new(AtomicUsize::new(0));

    batches
        .try_for_each_concurrent(None, |batch| {
            let current_writer = current_writer.clone();
            let mut writer_sender = writer_sender.clone();
            let num_bytes = num_bytes.clone();
            let object_store = object_store.clone();
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
                        create_arrow_writer(location, arrow_schema, object_store)
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
            async move {
                let metadata = writer.1.close().await?;
                let size = object_store
                    .head(&writer.0.as_str().into())
                    .await
                    .map_err(|err| ArrowError::from_external_error(err.into()))?
                    .size;
                Ok(parquet_to_datafile(
                    &writer.0,
                    size,
                    &metadata,
                    schema,
                    &partition_spec.fields,
                )?)
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

async fn create_arrow_writer(
    location: &str,
    schema: &arrow::datatypes::Schema,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>), ArrowError> {
    let mut rand = [0u8; 6];
    getrandom::getrandom(&mut rand)
        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
        .unwrap();

    let parquet_path =
        location.to_string() + "/data/" + &Uuid::now_v1(&rand).to_string() + ".parquet";

    let (_, writer) = object_store
        .put_multipart(&parquet_path.clone().into())
        .await
        .map_err(|err| ArrowError::from_external_error(err.into()))?;

    Ok((
        parquet_path,
        AsyncArrowWriter::try_new(writer, Arc::new(schema.clone()), 0, None)?,
    ))
}

#[inline]
fn record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .schema()
        .fields
        .iter()
        .fold(0, |acc, x| acc + x.size())
        * batch.num_rows()
}
