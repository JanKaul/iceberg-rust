use std::{cmp::Ordering, collections::HashSet};

use crate::{
    error::Error,
    table::manifest_list::ManifestListReader,
    util::{summary_to_rectangle, Rectangle},
};

use super::append::SelectedManifest;

/// Select the manifest that yields the smallest bounding rectangle after the
/// bounding rectangle of the new values has been added.
pub(crate) fn select_manifest_without_overwrites_partitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    bounding_partition_values: &Rectangle,
    overwrites: &HashSet<String>,
) -> Result<SelectedManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;

        let mut bounds =
            summary_to_rectangle(manifest.partitions.as_ref().ok_or(Error::NotFound(format!(
                "Partition struct in manifest {}",
                manifest.manifest_path
            )))?)?;

        bounds.expand(bounding_partition_values);

        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        let Some((selected_bounds, selected_manifest)) = &selected_state else {
            selected_state = Some((bounds, manifest));
            continue;
        };

        match selected_bounds.cmp_with_priority(&bounds)? {
            Ordering::Greater => {
                if !overwrites.contains(&selected_manifest.manifest_path) {
                    manifest_list_writer.append_ser(selected_manifest)?;
                }
                selected_state = Some((bounds, manifest));
                continue;
            }
            _ => {
                if !overwrites.contains(&manifest.manifest_path) {
                    manifest_list_writer.append_ser(manifest)?;
                }

                continue;
            }
        }
    }
    selected_state
        .map(|(_, entry)| SelectedManifest {
            manifest: entry,
            file_count_all_entries,
        })
        .ok_or(Error::NotFound("Manifest for insert".to_owned()))
}

/// Select the manifest with the smallest number of rows.
pub(crate) fn select_manifest_without_overwrites_unpartitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    overwrites: &HashSet<String>,
) -> Result<SelectedManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;
        // TODO: should this also account for existing_rows_count / existing_files_count?
        let row_count = manifest.added_rows_count;
        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        let Some((selected_row_count, selected_manifest)) = &selected_state else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        // If the file doesn't have any rows, we select it
        let Some(row_count) = row_count else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        if selected_row_count.is_some_and(|x| x > row_count) {
            if !overwrites.contains(&selected_manifest.manifest_path) {
                manifest_list_writer.append_ser(selected_manifest)?;
            }
            selected_state = Some((Some(row_count), manifest));
            continue;
        } else {
            if !overwrites.contains(&manifest.manifest_path) {
                manifest_list_writer.append_ser(manifest)?;
            }
            continue;
        }
    }
    selected_state
        .map(|(_, entry)| SelectedManifest {
            manifest: entry,
            file_count_all_entries,
        })
        .ok_or(Error::NotFound("Manifest for insert".to_owned()))
}
