use std::{cmp::Ordering, collections::HashSet};

use iceberg_rust_spec::manifest_list::ManifestListEntry;
use iceberg_rust_spec::table_metadata::TableMetadata;

use crate::{
    error::Error,
    table::manifest_list::{append_manifest, ManifestListReader, RowIdAssigner},
    table::transaction::append::can_reuse_for_current_writes,
    util::{summary_to_rectangle, Rectangle},
};

pub(crate) struct OverwriteManifest {
    pub manifest: Option<ManifestListEntry>,
    pub file_count_all_entries: usize,
    pub manifests_to_overwrite: Vec<ManifestListEntry>,
}

/// Select the manifest that yields the smallest bounding rectangle after the
/// bounding rectangle of the new values has been added.
pub(crate) fn select_manifest_without_overwrites_partitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    mut row_id_assigner: Option<&mut RowIdAssigner>,
    bounding_partition_values: &Rectangle,
    overwrites: &HashSet<String>,
    table_metadata: &TableMetadata,
) -> Result<OverwriteManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    let mut manifests_to_overwrite = Vec::new();
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;

        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;
        if manifest.content != iceberg_rust_spec::manifest_list::Content::Data
            || !can_reuse_for_current_writes(&manifest, table_metadata)
        {
            if overwrites.contains(&manifest.manifest_path) {
                manifests_to_overwrite.push(manifest);
            } else {
                append_manifest(
                    manifest_list_writer,
                    row_id_assigner.as_deref_mut(),
                    manifest,
                )?;
            }
            continue;
        }

        let mut bounds =
            summary_to_rectangle(manifest.partitions.as_ref().ok_or(Error::NotFound(format!(
                "Partition struct in manifest {}",
                manifest.manifest_path
            )))?)?;

        bounds.expand(bounding_partition_values);
        let Some((selected_bounds, _)) = &selected_state else {
            selected_state = Some((bounds, manifest));
            continue;
        };

        match selected_bounds.cmp_with_priority(&bounds)? {
            Ordering::Greater => {
                let old = selected_state.replace((bounds, manifest));
                if let Some((_, old)) = old {
                    if !overwrites.contains(&old.manifest_path) {
                        append_manifest(manifest_list_writer, row_id_assigner.as_deref_mut(), old)?;
                    } else {
                        manifests_to_overwrite.push(old);
                    }
                }
                continue;
            }
            _ => {
                if !overwrites.contains(&manifest.manifest_path) {
                    append_manifest(
                        manifest_list_writer,
                        row_id_assigner.as_deref_mut(),
                        manifest,
                    )?;
                } else {
                    manifests_to_overwrite.push(manifest);
                }

                continue;
            }
        }
    }
    Ok(OverwriteManifest {
        manifest: selected_state.map(|(_, entry)| entry),
        file_count_all_entries,
        manifests_to_overwrite,
    })
}

/// Select the manifest with the smallest number of rows.
pub(crate) fn select_manifest_without_overwrites_unpartitioned(
    manifest_list_reader: ManifestListReader<&[u8]>,
    manifest_list_writer: &mut apache_avro::Writer<Vec<u8>>,
    mut row_id_assigner: Option<&mut RowIdAssigner>,
    overwrites: &HashSet<String>,
    table_metadata: &TableMetadata,
) -> Result<OverwriteManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    let mut manifests_to_overwrite = Vec::new();
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;
        // TODO: should this also account for existing_rows_count / existing_files_count?
        let row_count = manifest.added_rows_count;
        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        if manifest.content != iceberg_rust_spec::manifest_list::Content::Data
            || !can_reuse_for_current_writes(&manifest, table_metadata)
        {
            if overwrites.contains(&manifest.manifest_path) {
                manifests_to_overwrite.push(manifest);
            } else {
                append_manifest(
                    manifest_list_writer,
                    row_id_assigner.as_deref_mut(),
                    manifest,
                )?;
            }
            continue;
        }

        let Some((selected_row_count, _)) = &selected_state else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        // If the file doesn't have any rows, we select it
        let Some(row_count) = row_count else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        if selected_row_count.is_some_and(|x| x > row_count) {
            let old = selected_state.replace((Some(row_count), manifest));
            if let Some((_, old)) = old {
                if !overwrites.contains(&old.manifest_path) {
                    append_manifest(manifest_list_writer, row_id_assigner.as_deref_mut(), old)?;
                } else {
                    manifests_to_overwrite.push(old);
                }
            }
            continue;
        } else {
            if !overwrites.contains(&manifest.manifest_path) {
                append_manifest(
                    manifest_list_writer,
                    row_id_assigner.as_deref_mut(),
                    manifest,
                )?;
            } else {
                manifests_to_overwrite.push(manifest);
            }
            continue;
        }
    }
    Ok(OverwriteManifest {
        manifest: selected_state.map(|(_, entry)| entry),
        file_count_all_entries,
        manifests_to_overwrite,
    })
}
