/*!
 * Functions to read arrow record batches from an iceberg table
*/

use std::{convert, ops::Range, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, Stream, StreamExt};
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader,
        ParquetRecordBatchStreamBuilder,
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    error::Error,
    object_store::{legacy_object_store_path_from_metadata, object_store_path_from_metadata},
};

use iceberg_rust_spec::spec::manifest::{FileFormat, ManifestEntry};

// A minimal [`AsyncFileReader`] over a data file in an [`ObjectStore`].
//
// Iceberg always knows a data file's size up front (it is tracked in the
// manifest entry), so unlike a general-purpose object-store reader this
// implementation never needs to fall back to suffix range requests to
// locate the Parquet footer.
// TODO: Consider wrapping this with `parquet::arrow::async_reader::SpawnedReader` at some point.
pub(crate) struct DataFileReader {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    legacy_path: Option<Path>,
    file_size: u64,
}

impl DataFileReader {
    pub(crate) fn new(object_store: Arc<dyn ObjectStore>, path: Path, file_size: u64) -> Self {
        Self {
            object_store,
            path,
            legacy_path: None,
            file_size,
        }
    }

    pub(crate) fn with_legacy_path(mut self, legacy_path: Option<Path>) -> Self {
        self.legacy_path = legacy_path;
        self
    }
}

impl AsyncFileReader for DataFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            match self.object_store.get_range(&self.path, range.clone()).await {
                Ok(bytes) => {
                    self.legacy_path = None;
                    Ok(bytes)
                }
                Err(err @ object_store::Error::NotFound { .. }) => {
                    let Some(legacy) = self.legacy_path.take() else {
                        return Err(ParquetError::External(Box::new(err)));
                    };
                    let bytes = self
                        .object_store
                        .get_range(&legacy, range)
                        .await
                        .map_err(|err| ParquetError::External(Box::new(err)))?;
                    self.path = legacy;
                    Ok(bytes)
                }
                Err(err) => Err(ParquetError::External(Box::new(err))),
            }
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            match self.object_store.get_ranges(&self.path, &ranges).await {
                Ok(bytes) => {
                    self.legacy_path = None;
                    Ok(bytes)
                }
                Err(err @ object_store::Error::NotFound { .. }) => {
                    let Some(legacy) = self.legacy_path.take() else {
                        return Err(ParquetError::External(Box::new(err)));
                    };
                    let bytes = self
                        .object_store
                        .get_ranges(&legacy, &ranges)
                        .await
                        .map_err(|err| ParquetError::External(Box::new(err)))?;
                    self.path = legacy;
                    Ok(bytes)
                }
                Err(err) => Err(ParquetError::External(Box::new(err))),
            }
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(options.map(|o| o.metadata_options().clone()))
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Read a parquet file into a stream of arrow recordbatches. The record batches are read asynchronously and are unordered
pub async fn read(
    manifest_files: impl Iterator<Item = ManifestEntry>,
    object_store: Arc<dyn ObjectStore>,
) -> impl Stream<Item = Result<RecordBatch, ParquetError>> {
    stream::iter(manifest_files)
        .then(move |manifest| {
            let object_store = object_store.clone();
            async move {
                let data_file = manifest.data_file();
                match data_file.file_format() {
                    FileFormat::Parquet => {
                        let location = data_file.file_path();
                        let path = object_store_path_from_metadata(location)
                            .map_err(|error| Error::InvalidFormat(error.to_string()))?;
                        let legacy_path = legacy_object_store_path_from_metadata(location, &path);
                        let object_reader = DataFileReader::new(
                            object_store,
                            path,
                            (*data_file.file_size_in_bytes()) as u64,
                        )
                        .with_legacy_path(legacy_path);
                        Ok::<_, Error>(
                            ParquetRecordBatchStreamBuilder::new(object_reader)
                                .await?
                                .build()?,
                        )
                    }
                    _ => Err(Error::NotSupported("fileformat".to_string())),
                }
            }
        })
        .map(|result| match result {
            Ok(reader) => reader.left_stream(),
            Err(error) => {
                stream::iter([Err(ParquetError::External(Box::new(error)))]).right_stream()
            }
        })
        .flat_map_unordered(None, convert::identity)
}

#[cfg(test)]
mod tests {
    use super::{read, DataFileReader};
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;
    use iceberg_rust_spec::spec::{
        manifest::{Content, DataFile, FileFormat, ManifestEntry, Status},
        table_metadata::FormatVersion,
        values::Struct,
    };
    use object_store::{memory::InMemory, path::Path, ObjectStoreExt};
    use parquet::arrow::{async_reader::AsyncFileReader, ArrowWriter};

    #[tokio::test]
    async fn encoded_data_file_reads_and_missing_files_fail() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = i64::try_from(bytes.len()).unwrap();

        let store = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("data/name=日😀/part.parquet"),
                bytes.clone().into(),
            )
            .await
            .unwrap();
        let entry = |location: &str| {
            let file = DataFile::builder()
                .with_content(Content::Data)
                .with_file_path(location.to_string())
                .with_file_format(FileFormat::Parquet)
                .with_partition(Struct {
                    fields: Vec::new(),
                    lookup: BTreeMap::new(),
                })
                .with_record_count(2)
                .with_file_size_in_bytes(file_size)
                .with_column_sizes(None)
                .with_value_counts(None)
                .with_null_value_counts(None)
                .with_nan_value_counts(None)
                .with_distinct_counts(None)
                .with_lower_bounds(None)
                .with_upper_bounds(None)
                .build()
                .unwrap();
            ManifestEntry::builder()
                .with_format_version(FormatVersion::V2)
                .with_status(Status::Added)
                .with_data_file(file)
                .build()
                .unwrap()
        };

        let encoded = entry("s3://bucket/data/name=%E6%97%A5%F0%9F%98%80/part.parquet");
        let batches = read(std::iter::once(encoded.clone()), store)
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.as_slice(), std::slice::from_ref(&batch));

        let legacy_store = Arc::new(InMemory::new());
        legacy_store
            .put(
                &Path::from("data/name=a%2Fb/part.parquet"),
                bytes.clone().into(),
            )
            .await
            .unwrap();
        let legacy = entry("s3://bucket/data/name=a%2Fb/part.parquet");
        let batches = read(std::iter::once(legacy), legacy_store)
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.as_slice(), std::slice::from_ref(&batch));

        let legacy_tilde_store = Arc::new(InMemory::new());
        legacy_tilde_store
            .put(
                &Path::from("data/name=a~b/part.parquet"),
                bytes.clone().into(),
            )
            .await
            .unwrap();
        let legacy_tilde = entry("s3://bucket/data/name=a~b/part.parquet");
        let batches = read(std::iter::once(legacy_tilde), legacy_tilde_store)
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.as_slice(), std::slice::from_ref(&batch));

        let old_location_store = Arc::new(InMemory::new());
        old_location_store
            .put(&Path::from("/table//data/part.parquet"), bytes.into())
            .await
            .unwrap();
        let old_location = entry("s3://bucket/table//data/part.parquet");
        let batches = read(std::iter::once(old_location), old_location_store)
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches, [batch]);

        let missing = read(std::iter::once(encoded), Arc::new(InMemory::new()))
            .await
            .try_collect::<Vec<_>>()
            .await;
        assert!(missing.is_err(), "missing data files must fail the scan");
    }

    #[tokio::test]
    async fn reader_does_not_switch_keys_after_successful_range() {
        let store = Arc::new(InMemory::new());
        let preferred = Path::parse("data/name=a%2Fb/part.parquet").unwrap();
        let legacy = Path::from("data/name=a%2Fb/part.parquet");
        store
            .put(&preferred, bytes::Bytes::from_static(b"ok").into())
            .await
            .unwrap();
        store
            .put(&legacy, bytes::Bytes::from_static(b"no").into())
            .await
            .unwrap();

        let mut reader =
            DataFileReader::new(store.clone(), preferred.clone(), 2).with_legacy_path(Some(legacy));
        assert_eq!(reader.get_bytes(0..2).await.unwrap().as_ref(), b"ok");
        store.delete(&preferred).await.unwrap();
        assert!(reader.get_byte_ranges(vec![0..1, 1..2]).await.is_err());
    }
}
