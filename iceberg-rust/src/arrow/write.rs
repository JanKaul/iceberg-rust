/*!
 * Functions to write arrow record batches to an iceberg table
*/

use futures::{
    channel::mpsc::{channel, unbounded, Receiver, Sender},
    lock::Mutex,
    SinkExt, StreamExt, TryStreamExt,
};
use object_store::{buffered::BufWriter, ObjectStore};
use std::fmt::Write;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use iceberg_rust_spec::{
    partition::BoundPartitionField,
    spec::{manifest::DataFile, schema::Schema, table_metadata::TableMetadata, values::Value},
    table_metadata::{WRITE_DATA_PATH, WRITE_OBJECT_STORAGE_ENABLED},
    util::strip_prefix,
};
use parquet::{
    arrow::AsyncArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use uuid::Uuid;

use crate::{catalog::bucket::Bucket, error::Error, file_format::parquet::parquet_to_datafile};

use super::partition::partition_record_batches;

const MAX_PARQUET_SIZE: usize = 512_000_000;

/// Partitions arrow record batches and writes them to parquet files. Does not perform any operation on an iceberg table.
pub async fn write_parquet_partitioned(
    metadata: &TableMetadata,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
    branch: Option<&str>,
) -> Result<Vec<DataFile>, ArrowError> {
    let schema = metadata.current_schema(branch).map_err(Error::from)?;
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

    let (mut sender, reciever) = unbounded();

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
            schema,
            &arrow_schema,
            partition_fields,
            partition_path,
            batches,
            object_store.clone(),
        )
        .await?;
        sender.send(files).await.map_err(Error::from)?;
    } else {
        let streams = partition_record_batches(batches, partition_fields).await?;

        streams
            .map(Ok::<_, ArrowError>)
            .try_for_each_concurrent(None, |(partition_values, batches)| {
                let arrow_schema = arrow_schema.clone();
                let object_store = object_store.clone();
                let mut sender = sender.clone();
                async move {
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
                    let files = write_parquet_files(
                        data_location,
                        schema,
                        &arrow_schema,
                        partition_fields,
                        partition_path,
                        batches,
                        object_store.clone(),
                    )
                    .await?;
                    sender.send(files).await.map_err(Error::from)?;
                    Ok(())
                }
            })
            .await?;
    }

    sender.close_channel();

    Ok(reciever
        .fold(Vec::new(), |mut acc, x| async move {
            acc.extend(x);
            acc
        })
        .await)
}

type SendableAsyncArrowWriter = AsyncArrowWriter<BufWriter>;
type ArrowSender = Sender<(String, SendableAsyncArrowWriter)>;
type ArrowReciever = Receiver<(String, SendableAsyncArrowWriter)>;

/// Write arrow record batches to parquet files. Does not perform any operation on an iceberg table.
async fn write_parquet_files(
    data_location: &str,
    schema: &Schema,
    arrow_schema: &ArrowSchema,
    partition_fields: &[BoundPartitionField<'_>],
    partition_path: Option<String>,
    batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<DataFile>, ArrowError> {
    let bucket = Bucket::from_path(data_location)?;
    let current_writer = Arc::new(Mutex::new(
        create_arrow_writer(
            data_location,
            partition_path.clone(),
            arrow_schema,
            object_store.clone(),
        )
        .await?,
    ));

    let (mut writer_sender, writer_reciever): (ArrowSender, ArrowReciever) = channel(32);

    let num_bytes = Arc::new(AtomicUsize::new(0));

    batches
        .try_for_each_concurrent(None, |batch| {
            let current_writer = current_writer.clone();
            let mut writer_sender = writer_sender.clone();
            let num_bytes = num_bytes.clone();
            let object_store = object_store.clone();
            let partition_path = partition_path.clone();
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
                        create_arrow_writer(
                            data_location,
                            partition_path,
                            arrow_schema,
                            object_store,
                        )
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
                    partition_fields,
                )?)
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

#[inline]
fn generate_partition_path(
    partition_fields: &[BoundPartitionField<'_>],
    partiton_values: &[Value],
) -> Result<String, ArrowError> {
    partition_fields
        .iter()
        .zip(partiton_values.iter())
        .map(|(field, value)| {
            let name = field.name().to_owned();
            Ok(name + "=" + &value.to_string() + "/")
        })
        .collect::<Result<String, ArrowError>>()
}

async fn create_arrow_writer(
    data_location: &str,
    partition_path: Option<String>,
    schema: &arrow::datatypes::Schema,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, AsyncArrowWriter<BufWriter>), ArrowError> {
    let mut rand = [0u8; 6];
    getrandom::getrandom(&mut rand)
        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
        .unwrap();

    let path = partition_path.unwrap_or_else(|| {
        rand[0..3]
            .iter()
            .fold(String::with_capacity(8), |mut acc, x| {
                write!(&mut acc, "{:x}", x).unwrap();
                acc
            })
            + "/"
    });

    let parquet_path =
        strip_prefix(data_location) + &path + &Uuid::now_v1(&rand).to_string() + ".parquet";

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
    fn test_generate_partiton_location_success() {
        let field = StructField {
            id: 0,
            name: "date".to_owned(),
            required: false,
            field_type: Type::Primitive(iceberg_rust_spec::types::PrimitiveType::Date),
            doc: None,
        };
        let partfield = PartitionField::new(1, 1001, "month", Transform::Month);
        let partition_fields = vec![BoundPartitionField::new(&partfield, &field)];
        let partiton_values = vec![Value::Int(10)];

        let result = super::generate_partition_path(&partition_fields, &partiton_values);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "month=10/");
    }
}
