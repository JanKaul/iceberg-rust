/*!
 * Functions to write arrow record batches to an iceberg table
*/

use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    lock::Mutex,
    stream, StreamExt, TryStreamExt,
};
use object_store::ObjectStore;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::AsyncWrite;

use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use parquet::arrow::AsyncArrowWriter;
use uuid::Uuid;

use crate::{
    file_format::DatafileMetadata,
    spec::{partition::PartitionSpec, schema::Schema},
};

use super::partition::partition_record_batches;

const MAX_PARQUET_SIZE: usize = 512_000_000;

/// Partitions arrow record batches and writes them to parquet files. Does not perform any operation on an iceberg table.
pub async fn write_parquet_partitioned(
    location: &str,
    schema: &Schema,
    partition_spec: &PartitionSpec,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<(String, DatafileMetadata)>, ArrowError> {
    let streams = partition_record_batches(batches, partition_spec, schema).await?;
    let arrow_schema: Arc<ArrowSchema> = Arc::new(
        (&schema.fields)
            .try_into()
            .map_err(|err: anyhow::Error| ArrowError::SchemaError(err.to_string()))?,
    );
    stream::iter(streams.into_iter())
        .then(|batches| {
            let arrow_schema = arrow_schema.clone();
            let object_store = object_store.clone();
            async move {
                write_parquet_files(location, &arrow_schema, batches, object_store.clone()).await
            }
        })
        .try_fold(vec![], |mut acc, x| async move {
            acc.extend_from_slice(&x);
            Ok(acc)
        })
        .await
}

/// Write arrow record batches to parquet files. Does not perform any operation on an iceberg table.
pub async fn write_parquet_files(
    location: &str,
    schema: &ArrowSchema,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<(String, DatafileMetadata)>, ArrowError> {
    let current_writer = Arc::new(Mutex::new(
        create_arrow_writer(location, schema, object_store.clone()).await?,
    ));

    let (mut writer_sender, writer_reciever): (
        Sender<(String, AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>)>,
        Receiver<(String, AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>)>,
    ) = channel(32);

    let num_bytes = Arc::new(AtomicUsize::new(0));

    batches
        .for_each_concurrent(None, |batch| {
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
                        create_arrow_writer(location, schema, object_store)
                            .await
                            .unwrap(),
                    );
                    writer_sender.try_send(finished_writer).unwrap();
                }
                if let Ok(batch) = batch {
                    current_writer.1.write(&batch).await.unwrap();
                    let batch_size = record_batch_size(&batch);
                    num_bytes.fetch_add(batch_size, Ordering::AcqRel);
                }
            }
        })
        .await;

    let last = Arc::try_unwrap(current_writer).unwrap().into_inner();

    writer_sender.try_send(last).unwrap();

    writer_sender.close_channel();

    writer_reciever
        .then(|writer| async move {
            let metadata = writer.1.close().await?;
            Ok::<_, ArrowError>((writer.0, DatafileMetadata::Parquet(metadata)))
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

fn record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .schema()
        .fields
        .iter()
        .fold(0, |acc, x| acc + x.size())
        * batch.num_rows()
}
