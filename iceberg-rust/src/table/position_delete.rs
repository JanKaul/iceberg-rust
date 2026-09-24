//! Streaming loading of Iceberg v2 position-delete Parquet files.

use std::{collections::HashMap, sync::Arc};

use arrow::array::{Array, Int64Array, LargeStringArray, StringArray, StringViewArray};
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust_spec::{
    arrow::schema::PARQUET_FIELD_ID_META_KEY,
    spec::{
        deletion_vector::DeletionVector,
        manifest::{FileFormat, ManifestEntry},
    },
    util,
};
use object_store::{path::Path, ObjectStore};
use parquet::arrow::{async_reader::ParquetRecordBatchStreamBuilder, ProjectionMask};
use roaring::RoaringTreemap;

use crate::{arrow::read::DataFileReader, error::Error};

const FILE_PATH_FIELD_ID: i32 = i32::MAX - 101;
const POSITION_FIELD_ID: i32 = i32::MAX - 102;
const MAX_CONCURRENT_DELETE_FILE_READS: usize = 8;

/// Load v2 position-delete files into the same path-keyed bitmap representation
/// used by v3 deletion vectors.
///
/// Only the reserved `file_path` and `pos` fields are decoded. Rows for data
/// files outside `active_data_sequence_numbers` are skipped, as are deletes
/// whose data sequence number is older than the referenced data file.
pub async fn load_position_deletes(
    entries: Vec<ManifestEntry>,
    active_data_sequence_numbers: Arc<HashMap<String, Option<i64>>>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, DeletionVector>, Error> {
    let mut partial_indexes = stream::iter(entries)
        .map(|entry| {
            let object_store = object_store.clone();
            let active_data_sequence_numbers = active_data_sequence_numbers.clone();
            async move {
                load_one_position_delete_file(entry, active_data_sequence_numbers, object_store)
                    .await
            }
        })
        .buffer_unordered(MAX_CONCURRENT_DELETE_FILE_READS);

    let mut merged: HashMap<String, RoaringTreemap> = HashMap::new();
    while let Some(partial) = partial_indexes.try_next().await? {
        for (path, positions) in partial {
            merged.entry(path).or_default().extend(positions);
        }
    }

    Ok(merged
        .into_iter()
        .map(|(path, positions)| (path, DeletionVector::from(positions)))
        .collect())
}

async fn load_one_position_delete_file(
    entry: ManifestEntry,
    active_data_sequence_numbers: Arc<HashMap<String, Option<i64>>>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, RoaringTreemap>, Error> {
    let data_file = entry.data_file();
    if data_file.file_format() != &FileFormat::Parquet {
        return Err(Error::NotSupported(format!(
            "position delete file format {:?}",
            data_file.file_format()
        )));
    }
    let delete_sequence_number = entry.sequence_number().ok_or_else(|| {
        Error::InvalidFormat(format!(
            "position delete file {} is missing its data sequence number",
            data_file.file_path()
        ))
    })?;
    let file_size = u64::try_from(*data_file.file_size_in_bytes()).map_err(|_| {
        Error::InvalidFormat(format!(
            "position delete file {} has a negative file size",
            data_file.file_path()
        ))
    })?;

    let reader = DataFileReader::new(
        object_store,
        Path::from(util::strip_prefix(data_file.file_path())),
        file_size,
    );
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let file_path_index = field_index_by_id(builder.schema(), FILE_PATH_FIELD_ID)?;
    let position_index = field_index_by_id(builder.schema(), POSITION_FIELD_ID)?;
    let projection =
        ProjectionMask::roots(builder.parquet_schema(), [file_path_index, position_index]);
    let (file_path_index, position_index) = if file_path_index < position_index {
        (0, 1)
    } else {
        (1, 0)
    };
    let mut batches = builder.with_projection(projection).build()?;

    let mut current_path: Option<PositionRun> = None;
    let mut positions_by_path: HashMap<String, RoaringTreemap> = HashMap::new();
    while let Some(batch) = batches.try_next().await? {
        let paths = batch.column(file_path_index);
        let positions = batch
            .column(position_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "position delete file {} field id {POSITION_FIELD_ID} must be INT64",
                    data_file.file_path()
                ))
            })?;

        for row in 0..batch.num_rows() {
            if paths.is_null(row) || positions.is_null(row) {
                return Err(Error::InvalidFormat(format!(
                    "position delete file {} contains a null file_path or pos",
                    data_file.file_path()
                )));
            }
            let raw_path = string_value(paths.as_ref(), row).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "position delete file {} field id {FILE_PATH_FIELD_ID} must be a string",
                    data_file.file_path()
                ))
            })?;
            if current_path
                .as_ref()
                .is_none_or(|current| current.raw_path != raw_path)
            {
                flush_position_run(&mut current_path, &mut positions_by_path);
                let normalized = util::strip_prefix(raw_path);
                let normalized_path = match active_data_sequence_numbers.get(&normalized) {
                    None => None,
                    Some(Some(sequence_number)) => {
                        delete_applies(delete_sequence_number, *sequence_number)
                            .then_some(normalized)
                    }
                    Some(None) => {
                        return Err(Error::InvalidFormat(format!(
                            "data file {normalized} referenced by a position delete is missing its data sequence number"
                        )));
                    }
                };
                current_path = Some(PositionRun {
                    raw_path: raw_path.to_owned(),
                    normalized_path,
                    positions: RoaringTreemap::new(),
                });
            }
            let position = positions.value(row);
            if position < 0 {
                return Err(Error::InvalidFormat(format!(
                    "position delete file {} contains negative position {position}",
                    data_file.file_path()
                )));
            }
            if let Some(current) = &mut current_path {
                if current.normalized_path.is_some() {
                    current.positions.insert(position as u64);
                }
            }
        }
    }
    flush_position_run(&mut current_path, &mut positions_by_path);

    Ok(positions_by_path)
}

struct PositionRun {
    raw_path: String,
    normalized_path: Option<String>,
    positions: RoaringTreemap,
}

fn flush_position_run(
    current_path: &mut Option<PositionRun>,
    positions_by_path: &mut HashMap<String, RoaringTreemap>,
) {
    let Some(PositionRun {
        normalized_path: Some(path),
        positions,
        ..
    }) = current_path.take()
    else {
        return;
    };
    positions_by_path.entry(path).or_default().extend(positions);
}

#[inline]
fn delete_applies(delete_sequence_number: i64, data_sequence_number: i64) -> bool {
    delete_sequence_number >= data_sequence_number
}

fn field_index_by_id(schema: &arrow::datatypes::Schema, field_id: i32) -> Result<usize, Error> {
    schema
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|value| value.parse::<i32>().ok())
                == Some(field_id)
        })
        .ok_or_else(|| {
            Error::InvalidFormat(format!(
                "position delete file is missing reserved field id {field_id}"
            ))
        })
}

fn string_value(array: &dyn Array, row: usize) -> Option<&str> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        Some(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        Some(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        Some(array.value(row))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use iceberg_rust_spec::spec::{
        manifest::{
            Content, DataFileBuilder, FileFormat, ManifestEntry, ManifestEntryBuilder, Status,
        },
        table_metadata::FormatVersion,
        values::Struct,
    };
    use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{delete_applies, load_position_deletes, FILE_PATH_FIELD_ID, POSITION_FIELD_ID};

    fn position_delete_parquet(rows: &[(&str, i64)]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("extra", DataType::Int32, false),
            Field::new("renamed_position", DataType::Int64, false).with_metadata(HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_FIELD_ID.to_string(),
                ),
            ])),
            Field::new("renamed_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FILE_PATH_FIELD_ID.to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![42; rows.len()])),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    async fn write_delete_entry(
        store: &dyn ObjectStore,
        path: &str,
        sequence_number: i64,
        rows: &[(&str, i64)],
    ) -> ManifestEntry {
        let bytes = position_delete_parquet(rows);
        let file_size = i64::try_from(bytes.len()).unwrap();
        store
            .put(&Path::from(path), PutPayload::from(bytes))
            .await
            .unwrap();
        let data_file = DataFileBuilder::default()
            .with_content(Content::PositionDeletes)
            .with_file_path(path.to_string())
            .with_file_format(FileFormat::Parquet)
            .with_partition(Struct {
                fields: Vec::new(),
                lookup: BTreeMap::new(),
            })
            .with_record_count(i64::try_from(rows.len()).unwrap())
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
        ManifestEntryBuilder::default()
            .with_format_version(FormatVersion::V2)
            .with_status(Status::Added)
            .with_sequence_number(sequence_number)
            .with_data_file(data_file)
            .build()
            .unwrap()
    }

    #[test]
    fn position_delete_sequence_rule_is_inclusive() {
        assert!(!delete_applies(6, 7));
        assert!(delete_applies(7, 7));
        assert!(delete_applies(8, 7));
    }

    #[tokio::test]
    async fn loads_reordered_fields_and_applies_sequence_numbers() {
        let store = Arc::new(InMemory::new());
        let entries = vec![
            write_delete_entry(
                store.as_ref(),
                "/deletes/old.parquet",
                6,
                &[("s3://bucket/data/a.parquet", 1)],
            )
            .await,
            write_delete_entry(
                store.as_ref(),
                "/deletes/equal.parquet",
                7,
                &[("s3://bucket/data/a.parquet", 2)],
            )
            .await,
            write_delete_entry(
                store.as_ref(),
                "/deletes/newer.parquet",
                8,
                &[
                    ("s3://bucket/data/b.parquet", 3),
                    ("s3://bucket/data/not-scanned.parquet", 4),
                ],
            )
            .await,
        ];
        let active = Arc::new(HashMap::from([
            ("/data/a.parquet".to_string(), Some(7)),
            ("/data/b.parquet".to_string(), Some(7)),
        ]));

        let index = load_position_deletes(entries, active, store).await.unwrap();

        assert_eq!(index.len(), 2);
        assert!(!index["/data/a.parquet"].is_deleted(1));
        assert!(index["/data/a.parquet"].is_deleted(2));
        assert!(index["/data/b.parquet"].is_deleted(3));
    }
}
