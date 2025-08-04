use iceberg_rust_spec::{
    manifest::Content, manifest::DataFile, manifest::ManifestEntry, manifest_list,
    manifest_list::ManifestListEntry,
};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{
    error::Error,
    table::manifest_list::ManifestListReader,
    util::{cmp_with_priority, partition_struct_to_vec, summary_to_rectangle, try_sub, Rectangle},
};

/// Split sets of datafiles depending on their partition_values
#[allow(clippy::type_complexity)]
fn split_datafiles_once(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle,
    names: &[&str],
) -> Result<[(Vec<ManifestEntry>, Rectangle); 2], Error> {
    if let Ordering::Equal = cmp_with_priority(&rect.min, &rect.max)? {
        let mut smaller = files.collect::<Result<Vec<_>, Error>>()?;
        let larger = smaller.split_off(smaller.len() / 2);

        return Ok([
            (smaller, Rectangle::new(SmallVec::new(), SmallVec::new())),
            (larger, Rectangle::new(SmallVec::new(), SmallVec::new())),
        ]);
    }

    let mut smaller = Vec::new();
    let mut larger = Vec::new();
    let mut smaller_rect = None;
    let mut larger_rect = None;

    for manifest_entry in files {
        let manifest_entry = manifest_entry?;
        let position = partition_struct_to_vec(manifest_entry.data_file().partition(), names)?;
        // Compare distance to upper and lower bound. Since you can't compute a "norm" for a multidimensional vector where the dimensions have different datatypes,
        // the dimensions are compared individually and the norm is computed by weighing the earlier columns more than the later.
        if let Ordering::Greater = cmp_with_priority(
            &try_sub(&position, &rect.min)?,
            &try_sub(&rect.max, &position)?,
        )? {
            // if closer to upper bound
            larger.push(manifest_entry);

            if larger_rect.is_none() {
                larger_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(larger_rect) = larger_rect.as_mut() {
                larger_rect.expand_with_node(position);
            }
        } else {
            // if closer to lower bound
            smaller.push(manifest_entry);

            if smaller_rect.is_none() {
                smaller_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(smaller_rect) = smaller_rect.as_mut() {
                smaller_rect.expand_with_node(position);
            }
        }
    }
    Ok([
        (
            smaller,
            smaller_rect.expect("No files selected for the smaller rectangle"),
        ),
        (
            larger,
            larger_rect.expect("No files selected for the smaller rectangle"),
        ),
    ])
}

/// Splits the datafiles *n_split* times to decrease the number of datafiles per maniefst. Returns *2^n_splits* lists of manifest entries.
pub(crate) fn split_datafiles(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle,
    names: &[&str],
    n_split: u32,
) -> Result<Vec<Vec<ManifestEntry>>, Error> {
    let [(smaller, smaller_rect), (larger, larger_rect)] =
        split_datafiles_once(files, rect, names)?;
    if n_split == 1 {
        Ok(vec![smaller, larger])
    } else {
        let mut smaller = split_datafiles(
            smaller.into_iter().map(Ok),
            smaller_rect,
            names,
            n_split - 1,
        )?;
        let mut larger =
            split_datafiles(larger.into_iter().map(Ok), larger_rect, names, n_split - 1)?;

        smaller.append(&mut larger);
        Ok(smaller)
    }
}

pub(crate) struct SelectedManifest {
    pub manifest: Option<ManifestListEntry>,
    pub file_count_all_entries: usize,
}

/// Select the manifest that yields the smallest bounding rectangle after the
/// bounding rectangle of the new values has been added.
/// Only entries with matching `content` are considered as candidates.
pub(crate) fn select_manifest_partitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    bounding_partition_values: &Rectangle,
    content: manifest_list::Content,
) -> Result<SelectedManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let next_manifest = manifest_res?;

        let mut next_bounds = summary_to_rectangle(next_manifest.partitions.as_ref().ok_or(
            Error::NotFound(format!(
                "Partition struct in manifest {}",
                next_manifest.manifest_path
            )),
        )?)?;

        next_bounds.expand(bounding_partition_values);

        file_count_all_entries += next_manifest.added_files_count.unwrap_or(0) as usize;

        if next_manifest.content != content {
            // We only want to continue writing into an entry with `content` content
            manifest_list_writer.append_ser(next_manifest)?;
            continue;
        }

        match selected_state {
            None => {
                // Always select the first manifest
                selected_state = Some((next_bounds, next_manifest));
            }
            Some((selected_bounds, manifest)) => {
                match selected_bounds.cmp_with_priority(&next_bounds)? {
                    Ordering::Greater => {
                        manifest_list_writer.append_ser(manifest)?;
                        selected_state = Some((next_bounds, next_manifest));
                    }
                    _ => {
                        manifest_list_writer.append_ser(next_manifest)?;
                        selected_state = Some((selected_bounds, manifest));
                    }
                }
            }
        }
    }

    Ok(SelectedManifest {
        manifest: selected_state.map(|(_, manifest_list_entry)| manifest_list_entry),
        file_count_all_entries,
    })
}

/// Select the manifest with the smallest number of rows.
/// Only entries with matching `content` are considered as candidates.
pub(crate) fn select_manifest_unpartitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    content: manifest_list::Content,
) -> Result<SelectedManifest, Error> {
    let mut selected_manifest = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let next_manifest = manifest_res?;
        // TODO: should this also account for existing_rows_count / existing_files_count?
        // TODO: should we be adding the delete entries file count too?
        file_count_all_entries += next_manifest.added_files_count.unwrap_or(0) as usize;

        if next_manifest.content != content {
            // We only want to continue writing into an entry with `content` content
            manifest_list_writer.append_ser(next_manifest)?;
            continue;
        }

        match selected_manifest {
            None => {
                // Always select the first manifest
                selected_manifest = Some(next_manifest);
            }
            Some(manifest) => {
                // Always select the manifest list entry with the smaller number of rows
                let selected_rows_count = manifest.added_rows_count.unwrap_or(0);
                let next_rows_count = next_manifest.added_rows_count.unwrap_or(0);

                if next_rows_count < selected_rows_count {
                    manifest_list_writer.append_ser(manifest)?;
                    selected_manifest = Some(next_manifest);
                } else {
                    manifest_list_writer.append_ser(next_manifest)?;
                    selected_manifest = Some(manifest);
                }
            }
        }
    }
    Ok(SelectedManifest {
        manifest: selected_manifest,
        file_count_all_entries,
    })
}

pub(crate) fn append_summary(files: &[DataFile]) -> Option<HashMap<String, String>> {
    if files.is_empty() {
        return None;
    }

    let (mut added_data_files, mut added_records, mut added_files_size) = (0usize, 0i64, 0i64);

    for file in files.iter().filter(|f| *f.content() == Content::Data) {
        added_data_files += 1;
        added_records += file.record_count();
        added_files_size += file.file_size_in_bytes();
    }

    Some(HashMap::from([
        ("added-files-size".into(), added_files_size.to_string()),
        ("added-records".into(), added_records.to_string()),
        ("added-data-files".into(), added_data_files.to_string()),
    ]))
}
