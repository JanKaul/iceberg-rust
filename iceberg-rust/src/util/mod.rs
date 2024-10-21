use std::{cmp::Ordering, convert::identity};

use iceberg_rust_spec::{
    manifest_list::FieldSummary,
    values::{Struct, TryAdd, TrySub, Value},
};
use smallvec::SmallVec;

use crate::error::Error;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Rectangle<C> {
    pub min: SmallVec<[C; 4]>,
    pub max: SmallVec<[C; 4]>,
}

impl<C> Rectangle<C>
where
    C: PartialOrd + Clone + TryAdd + TrySub,
{
    pub(crate) fn new(min: SmallVec<[C; 4]>, max: SmallVec<[C; 4]>) -> Self {
        Self { min, max }
    }

    pub(crate) fn expand(&mut self, rect: &Rectangle<C>) {
        for i in 0..self.min.len() {
            if rect.min[i] < self.min[i] {
                self.min[i] = rect.min[i].clone();
            }
            if rect.max[i] > self.max[i] {
                self.max[i] = rect.max[i].clone();
            }
        }
    }

    pub(crate) fn expand_with_node(&mut self, node: SmallVec<[C; 4]>) {
        for i in 0..self.min.len() {
            if node[i] < self.min[i] {
                self.min[i] = node[i].clone();
            }
            if node[i] > self.max[i] {
                self.max[i] = node[i].clone();
            }
        }
    }

    /// Determine of one rectangle is larger than the other
    pub(crate) fn cmp_with_priority(&self, other: &Rectangle<C>) -> Result<Ordering, Error> {
        if self.contains(other) {
            Ok(Ordering::Greater)
        } else if other.contains(self) {
            Ok(Ordering::Less)
        } else {
            let mut self_iter = self
                .max
                .iter()
                .zip(self.min.iter())
                .map(|(max, min)| max.try_sub(min));
            let mut other_iter = other
                .max
                .iter()
                .zip(other.min.iter())
                .map(|(max, min)| max.try_sub(min));
            while let (Some(own), Some(other)) = (self_iter.next(), other_iter.next()) {
                let ordering = own?
                    .partial_cmp(&other?)
                    .ok_or(Error::InvalidFormat("Types for Partial Order".to_owned()))?;
                let Ordering::Equal = ordering else {
                    return Ok(ordering);
                };
            }
            Ok(Ordering::Equal)
        }
    }

    pub(crate) fn contains(&self, rect: &Rectangle<C>) -> bool {
        if self.min.len() == 0 {
            return false;
        }
        for i in 0..self.min.len() {
            if rect.min[i] < self.min[i] || rect.max[i] > self.max[i] {
                return false;
            }
        }
        true
    }

    pub(crate) fn intersects(&self, rect: &Rectangle<C>) -> bool {
        if self.min.len() == 0 {
            return false;
        }
        for i in 0..self.min.len() {
            if rect.min[i] > self.max[i] || rect.max[i] < self.min[i] {
                return false;
            }
        }
        true
    }
}

pub(crate) fn struct_to_smallvec(
    s: &Struct,
    names: &SmallVec<[&str; 4]>,
) -> Result<SmallVec<[Value; 4]>, Error> {
    names
        .iter()
        .map(|x| {
            s.get(*x)
                .and_then(|x| identity(x).as_ref().map(Clone::clone))
        })
        .collect::<Option<SmallVec<_>>>()
        .ok_or(Error::InvalidFormat("Partition struct".to_owned()))
}

pub(crate) fn summary_to_rectangle(summaries: &[FieldSummary]) -> Result<Rectangle<Value>, Error> {
    let mut max = SmallVec::with_capacity(summaries.len());
    let mut min = SmallVec::with_capacity(summaries.len());

    for summary in summaries {
        max.push(
            summary
                .upper_bound
                .clone()
                .ok_or(Error::NotFound("Partition".to_owned(), "struct".to_owned()))?,
        );
        min.push(
            summary
                .lower_bound
                .clone()
                .ok_or(Error::NotFound("Partition".to_owned(), "struct".to_owned()))?,
        );
    }

    Ok(Rectangle::new(min, max))
}
