/*!
 * Functions to read arrow record batches from an iceberg table
*/

use std::{convert, sync::Arc};

use arrow::record_batch::RecordBatch;
use futures::{stream, Stream, StreamExt};
use iceberg_rust_spec::util;
use object_store::ObjectStore;
use parquet::{
    arrow::{async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
};

use crate::error::Error;

use iceberg_rust_spec::spec::manifest::{FileFormat, ManifestEntry};

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
                        let object_reader = ParquetObjectReader::new(
                            object_store,
                            util::strip_prefix(data_file.file_path()).into(),
                        )
                        .with_file_size((*data_file.file_size_in_bytes()) as u64);
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
