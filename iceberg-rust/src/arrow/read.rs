/*!
 * Functions to read arrow record batches from an iceberg table
*/

use std::{convert, ops::Range, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, Stream, StreamExt, TryFutureExt};
use iceberg_rust_spec::util;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader,
        ParquetRecordBatchStreamBuilder,
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::error::Error;

use iceberg_rust_spec::spec::manifest::{FileFormat, ManifestEntry};

/// A minimal [`AsyncFileReader`] over a data file in an [`ObjectStore`].
///
/// Iceberg always knows a data file's size up front (it is tracked in the
/// manifest entry), so unlike a general-purpose object-store reader this
/// implementation never needs to fall back to suffix range requests to
/// locate the Parquet footer.
struct DataFileReader {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
}

impl AsyncFileReader for DataFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.object_store
            .get_range(&self.path, range)
            .map_err(|err| ParquetError::External(Box::new(err)))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            self.object_store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
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
                        let object_reader = DataFileReader {
                            object_store,
                            path: util::strip_prefix(data_file.file_path()).into(),
                            file_size: (*data_file.file_size_in_bytes()) as u64,
                        };
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
        .filter_map(|x| async move { x.ok() })
        .flat_map_unordered(None, convert::identity)
}
