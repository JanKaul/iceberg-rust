use std::cmp::Ordering;

use iceberg_rust_spec::{
    manifest::ManifestEntry,
    manifest_list::{ManifestListEntry, ManifestListReader},
};
use smallvec::SmallVec;

use crate::{
    error::Error,
    util::{cmp_with_priority, partition_struct_to_vec, summary_to_rectangle, try_sub, Rectangle},
};

/// Split sets of datafiles depending on their partition_values
#[allow(clippy::type_complexity)]
fn split_datafiles_once(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle,
    names: &[&str],
) -> Result<[(Vec<ManifestEntry>, Rectangle); 2], Error> {
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
            smaller_rect.ok_or(Error::NotFound("Lower".to_owned(), "rectangle".to_owned()))?,
        ),
        (
            larger,
            larger_rect.ok_or(Error::NotFound("Upper".to_owned(), "rectangle".to_owned()))?,
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

/// Select the manifest that yields the smallest bounding rectangle after the bounding rectangle of the new values has been added.
pub(crate) fn select_manifest(
    partition_column_names: &SmallVec<[&str; 4]>,
    mut manifest_list_reader: ManifestListReader<&[u8]>,
    file_count: &mut usize,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    bounding_partition_values: &Rectangle,
) -> Result<ManifestListEntry, Error> {
    let manifest =
        if partition_column_names.is_empty() {
            // Find the manifest with the lowest row count
            manifest_list_reader
                .try_fold(None, |acc, x| {
                    let manifest = x?;

                    let row_count = manifest.added_rows_count;

                    *file_count += manifest.added_files_count.unwrap_or(0) as usize;

                    let Some((old_row_count, old_manifest)) = acc else {
                        return Ok::<_, Error>(Some((row_count, manifest)));
                    };

                    let Some(row_count) = row_count else {
                        return Ok(Some((old_row_count, old_manifest)));
                    };

                    if old_row_count.is_none() || old_row_count.is_some_and(|x| x > row_count) {
                        manifest_list_writer.append_ser(old_manifest)?;
                        Ok(Some((Some(row_count), manifest)))
                    } else {
                        manifest_list_writer.append_ser(manifest)?;
                        Ok(Some((old_row_count, old_manifest)))
                    }
                })?
                .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))?
                .1
        } else {
            // Find the manifest with the smallest bounding partition values
            manifest_list_reader
                .try_fold(None, |acc, x| {
                    let manifest = x?;

                    let mut bounds =
                        summary_to_rectangle(manifest.partitions.as_ref().ok_or(
                            Error::NotFound("Partition".to_owned(), "struct".to_owned()),
                        )?)?;

                    bounds.expand(bounding_partition_values);

                    *file_count += manifest.added_files_count.unwrap_or(0) as usize;

                    let Some((old_bounds, old_manifest)) = acc else {
                        return Ok::<_, Error>(Some((bounds, manifest)));
                    };

                    match old_bounds.cmp_with_priority(&bounds)? {
                        Ordering::Greater => {
                            manifest_list_writer.append_ser(old_manifest)?;
                            Ok(Some((bounds, manifest)))
                        }
                        _ => {
                            manifest_list_writer.append_ser(manifest)?;
                            Ok(Some((old_bounds, old_manifest)))
                        }
                    }
                })?
                .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))?
                .1
        };
    Ok(manifest)
}
