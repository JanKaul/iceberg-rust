/*!
 * Functions to write arrow record batches to an iceberg table
*/

use futures::{lock::Mutex, stream::StreamExt};
use std::{pin::Pin, sync::Arc};

use anyhow::{anyhow, Result};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::Stream;
use parquet::arrow::AsyncArrowWriter;
use uuid::Uuid;

use crate::table::Table;

/// Write arrow record batches to an iceberg table
pub async fn write_single_parquet(
    table: &mut Table,
    batches: Pin<Box<dyn Stream<Item = Result<RecordBatch>>>>,
) -> Result<()> {
    let location = table.metadata().location();
    let object_store = table.object_store();

    let mut rand = [0u8; 6];
    getrandom::getrandom(&mut rand)
        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
        .unwrap();

    let parquet_path =
        location.to_string() + "/data/" + &Uuid::now_v1(&rand).to_string() + ".parquet";

    let (_, writer) = object_store
        .put_multipart(&parquet_path.clone().into())
        .await
        .map_err(|err| anyhow::Error::msg(err))?;

    let arrow_schema = table.metadata().current_schema().try_into()?;

    let arrow_writer = Arc::new(Mutex::new(AsyncArrowWriter::try_new(
        writer,
        Arc::new(arrow_schema),
        0,
        None,
    )?));

    batches
        .for_each_concurrent(None, |batch| {
            let arrow_writer = arrow_writer.clone();
            async move {
                if let Ok(batch) = batch {
                    arrow_writer.lock().await.write(&batch).await.unwrap();
                }
            }
        })
        .await;

    Arc::try_unwrap(arrow_writer)
        .map_err(|_| anyhow!("Failed to unwrap Arc"))?
        .into_inner()
        .close()
        .await?;

    table
        .new_transaction()
        .append(vec![parquet_path])
        .commit()
        .await
}
