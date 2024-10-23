use std::cmp::Ordering;

use iceberg_rust_spec::{manifest::ManifestEntry, values::Value};
use smallvec::{smallvec, SmallVec};

use crate::{
    error::Error,
    util::{cmp_dist, struct_to_smallvec, sub, Rectangle},
};

/// Split sets of datafiles depending on their partition_values
pub(crate) fn split_datafiles_once(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle<Value>,
    names: &SmallVec<[&str; 4]>,
) -> Result<[(Vec<ManifestEntry>, Rectangle<Value>); 2], Error> {
    let mut smaller = Vec::new();
    let mut larger = Vec::new();
    let mut smaller_rect = None;
    let mut larger_rect = None;

    for manifest_entry in files {
        let manifest_entry = manifest_entry?;
        let position = struct_to_smallvec(manifest_entry.data_file().partition(), names)?;
        // Check distance to upper and lower bound
        if let Ordering::Greater =
            cmp_dist(&sub(&position, &rect.min)?, &sub(&rect.max, &position)?)?
        {
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

pub(crate) fn split_datafiles(
    files: impl Iterator<Item = Result<ManifestEntry, Error>>,
    rect: Rectangle<Value>,
    names: &SmallVec<[&str; 4]>,
    n_split: u32,
) -> Result<SmallVec<[Vec<ManifestEntry>; 2]>, Error> {
    let [(smaller, smaller_rect), (larger, larger_rect)] =
        split_datafiles_once(files, rect, names)?;
    if n_split == 1 {
        Ok(smallvec![smaller, larger])
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
