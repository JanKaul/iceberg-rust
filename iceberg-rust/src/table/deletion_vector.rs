//! Eager loading of Iceberg v3 deletion vectors from Puffin files.
//!
//! Given a set of manifest entries that point at deletion-vector blobs, this
//! module fetches each blob's byte range from object storage in parallel and
//! returns an index keyed by the data file the bitmap applies to.
//!
//! The byte range is taken from the manifest entry's `content_offset` and
//! `content_size_in_bytes` so the Puffin footer never has to be parsed —
//! this matches Apache Iceberg Java's `BaseDeleteLoader`.

use std::{collections::HashMap, ops::Range, sync::Arc};

use futures::future::try_join_all;
use iceberg_rust_spec::{
    spec::{deletion_vector::DeletionVector, manifest::ManifestEntry},
    util,
};
use object_store::{path::Path, ObjectStore, ObjectStoreExt};

use crate::error::Error;

/// Fetch every deletion vector referenced by `entries` and return them keyed
/// by the absolute path of the data file each vector applies to.
///
/// Entries that are missing any of `referenced_data_file`, `content_offset`,
/// or `content_size_in_bytes` are rejected with [`Error::InvalidFormat`].
/// Two entries pointing at the same data file are also rejected: the Iceberg
/// spec allows at most one DV per data file per snapshot.
///
/// # Arguments
/// * `entries` — manifest entries whose `data_file().content()` is
///   `PositionDeletes` and whose `file_format()` is `Puffin`.
/// * `object_store` — store used to fetch the Puffin byte ranges.
///
/// # Errors
/// * Object-store fetch fails.
/// * Range bytes returned are not a valid `deletion-vector-v1` blob.
/// * Two entries claim the same `referenced_data_file`.
pub async fn load_deletion_vectors(
    entries: &[ManifestEntry],
    object_store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, DeletionVector>, Error> {
    let fetches = entries.iter().map(|entry| {
        let object_store = object_store.clone();
        async move { fetch_one(entry, object_store).await }
    });
    let results = try_join_all(fetches).await?;

    let mut index = HashMap::with_capacity(results.len());
    for (path, vector) in results {
        if index.insert(path.clone(), vector).is_some() {
            return Err(Error::InvalidFormat(format!(
                "more than one deletion vector references data file {path}"
            )));
        }
    }
    Ok(index)
}

async fn fetch_one(
    entry: &ManifestEntry,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, DeletionVector), Error> {
    let data_file = entry.data_file();
    let referenced = data_file.referenced_data_file().clone().ok_or_else(|| {
        Error::InvalidFormat("deletion vector manifest entry is missing referenced_data_file".into())
    })?;
    let offset = data_file.content_offset().ok_or_else(|| {
        Error::InvalidFormat(format!(
            "deletion vector for {referenced} is missing content_offset"
        ))
    })?;
    let size = data_file.content_size_in_bytes().ok_or_else(|| {
        Error::InvalidFormat(format!(
            "deletion vector for {referenced} is missing content_size_in_bytes"
        ))
    })?;
    if offset < 0 || size <= 0 {
        return Err(Error::InvalidFormat(format!(
            "deletion vector for {referenced} has invalid range offset={offset} size={size}"
        )));
    }

    let range = Range {
        start: offset as u64,
        end: (offset as u64) + (size as u64),
    };
    // Fetch bytes from the Puffin file (`data_file.file_path()`). The returned
    // tuple keys the bitmap by the normalized parquet path
    // (`referenced_data_file`), so the lookup in `table_scan` —
    // `dv_index.get(strip_prefix(data_file.file_path()))` — finds this entry
    // even when writer normalization differs from reader normalization
    // (e.g. scheme variations or trailing slashes).
    let puffin_path: Path = util::strip_prefix(data_file.file_path()).into();
    let bytes = object_store.get_range(&puffin_path, range).await?;
    let vector = DeletionVector::try_from(bytes.as_ref())?;
    Ok((util::strip_prefix(&referenced), vector))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use iceberg_rust_spec::spec::{
        deletion_vector::DeletionVector,
        manifest::{Content, DataFileBuilder, FileFormat, ManifestEntry, ManifestEntryBuilder, Status},
        puffin::{Blob, FileMetadata, PuffinWriter, STANDARD_BLOB_TYPE_DELETION_VECTOR_V1},
        table_metadata::FormatVersion,
        values::Struct,
    };
    use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload};
    use roaring::RoaringTreemap;

    use super::load_deletion_vectors;

    fn make_entry(
        puffin_path: &str,
        referenced: &str,
        offset: i64,
        size: i64,
    ) -> ManifestEntry {
        let data_file = DataFileBuilder::default()
            .with_content(Content::PositionDeletes)
            .with_file_path(puffin_path.to_string())
            // The spec assigns format=PUFFIN to DV entries; this crate's
            // FileFormat enum does not yet carry that variant, and our loader
            // does not branch on it, so Parquet is a safe placeholder here.
            .with_file_format(FileFormat::Parquet)
            .with_partition(Struct {
                fields: Vec::new(),
                lookup: BTreeMap::new(),
            })
            .with_record_count(1)
            .with_file_size_in_bytes(size)
            .with_column_sizes(None)
            .with_value_counts(None)
            .with_null_value_counts(None)
            .with_nan_value_counts(None)
            .with_distinct_counts(None)
            .with_lower_bounds(None)
            .with_upper_bounds(None)
            .with_referenced_data_file(Some(referenced.to_string()))
            .with_content_offset(Some(offset))
            .with_content_size_in_bytes(Some(size))
            .build()
            .unwrap();
        ManifestEntryBuilder::default()
            .with_format_version(FormatVersion::V3)
            .with_status(Status::Added)
            .with_data_file(data_file)
            .build()
            .unwrap()
    }

    fn dv_from(positions: &[u64]) -> DeletionVector {
        let mut tm = RoaringTreemap::new();
        for p in positions {
            tm.insert(*p);
        }
        DeletionVector::from(tm)
    }

    async fn write_puffin(
        store: &dyn ObjectStore,
        path: &str,
        blobs: &[&DeletionVector],
    ) -> Vec<(i64, i64)> {
        let mut writer = PuffinWriter::new();
        for dv in blobs {
            let payload = dv.to_bytes().unwrap();
            writer
                .write_blob(Blob {
                    blob_type: STANDARD_BLOB_TYPE_DELETION_VECTOR_V1.to_string(),
                    fields: vec![],
                    snapshot_id: -1,
                    sequence_number: -1,
                    compression_codec: None,
                    properties: Default::default(),
                    payload: &payload,
                })
                .unwrap();
        }
        let bytes = writer.finish().unwrap();
        store
            .put(&Path::from(path), PutPayload::from(bytes.clone()))
            .await
            .unwrap();
        // Re-read the footer to get the authoritative (offset, length) tuples.
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        meta.blobs.iter().map(|b| (b.offset, b.length)).collect()
    }

    #[tokio::test]
    async fn round_trip_single_vector() {
        let store = Arc::new(InMemory::new());
        let dv = dv_from(&[1, 5, 100]);
        let blobs = write_puffin(&*store, "/dvs/a.puffin", &[&dv]).await;
        let (offset, size) = blobs[0];
        let entry = make_entry("/dvs/a.puffin", "/data/f1.parquet", offset, size);

        let index = load_deletion_vectors(&[entry], store).await.unwrap();
        assert_eq!(index.len(), 1);
        let loaded = index.get("/data/f1.parquet").unwrap();
        assert_eq!(*loaded, dv);
    }

    #[tokio::test]
    async fn loads_multiple_blobs_from_one_file() {
        let store = Arc::new(InMemory::new());
        let dv_a = dv_from(&[1, 2, 3]);
        let dv_b = dv_from(&[1_000_000, 1_000_001]);
        let blobs = write_puffin(&*store, "/dvs/multi.puffin", &[&dv_a, &dv_b]).await;
        let entries = vec![
            make_entry(
                "/dvs/multi.puffin",
                "/data/a.parquet",
                blobs[0].0,
                blobs[0].1,
            ),
            make_entry(
                "/dvs/multi.puffin",
                "/data/b.parquet",
                blobs[1].0,
                blobs[1].1,
            ),
        ];

        let index = load_deletion_vectors(&entries, store).await.unwrap();
        assert_eq!(*index.get("/data/a.parquet").unwrap(), dv_a);
        assert_eq!(*index.get("/data/b.parquet").unwrap(), dv_b);
    }

    #[tokio::test]
    async fn rejects_duplicate_referenced_data_file() {
        let store = Arc::new(InMemory::new());
        let dv = dv_from(&[1]);
        let blobs = write_puffin(&*store, "/dvs/dup.puffin", &[&dv, &dv]).await;
        let entries = vec![
            make_entry("/dvs/dup.puffin", "/data/same.parquet", blobs[0].0, blobs[0].1),
            make_entry("/dvs/dup.puffin", "/data/same.parquet", blobs[1].0, blobs[1].1),
        ];
        let err = load_deletion_vectors(&entries, store).await.unwrap_err();
        match err {
            crate::error::Error::InvalidFormat(msg) => {
                assert!(msg.contains("more than one"), "got: {msg}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
