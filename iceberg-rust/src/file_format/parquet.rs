/*!
 * Helpers for parquet files
*/

use std::sync::Arc;

use anyhow::Result;

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::{
    arrow::async_reader::fetch_parquet_metadata, errors::ParquetError,
    file::metadata::ParquetMetaData,
};

/// Get parquet metadata for a given path from the object_store
pub async fn parquet_metadata(
    paths: Vec<String>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<(String, ObjectMeta, ParquetMetaData)>> {
    stream::iter(paths.into_iter())
        .then(|x| {
            let object_store = object_store.clone();
            async move {
                let path: Path = x.as_str().into();
                let file_metadata = object_store.head(&path).await.map_err(anyhow::Error::msg)?;
                let parquet_metadata = fetch_parquet_metadata(
                    |range| {
                        object_store
                            .get_range(&path, range)
                            .map_err(|err| ParquetError::General(err.to_string()))
                    },
                    file_metadata.size,
                    None,
                )
                .await
                .map_err(anyhow::Error::msg)?;
                Ok::<_, anyhow::Error>((x, file_metadata, parquet_metadata))
            }
        })
        .try_collect::<Vec<_>>()
        .await
}
