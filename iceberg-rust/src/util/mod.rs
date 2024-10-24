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
    C: PartialOrd + Ord + Clone + TryAdd + TrySub,
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
        if self.min.is_empty() {
            return false;
        }
        for i in 0..self.min.len() {
            if rect.min[i] < self.min[i] || rect.max[i] > self.max[i] {
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
        .map(|x| s.get(x).and_then(|x| identity(x).clone()))
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
                .ok_or(Error::NotFound("Upper".to_owned(), "bounds".to_owned()))?,
        );
        min.push(
            summary
                .lower_bound
                .clone()
                .ok_or(Error::NotFound("Lower".to_owned(), "bounds".to_owned()))?,
        );
    }

    Ok(Rectangle::new(min, max))
}

pub(crate) fn cmp_dist<C: PartialOrd>(left: &[C], right: &[C]) -> Result<Ordering, Error> {
    for (own, other) in left.iter().zip(right.iter()) {
        let ordering = own
            .partial_cmp(other)
            .ok_or(Error::InvalidFormat("Types for Partial Order".to_owned()))?;
        let Ordering::Equal = ordering else {
            return Ok(ordering);
        };
    }
    Ok(Ordering::Equal)
}

pub(crate) fn sub<C: TrySub>(left: &[C], right: &[C]) -> Result<SmallVec<[C; 4]>, Error> {
    let mut v = SmallVec::with_capacity(left.len());
    for i in 0..left.len() {
        v.push(left[i].try_sub(&right[i])?);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use iceberg_rust_spec::values::Value;
    use smallvec::smallvec;

    use super::*;

    #[test]
    fn test_sub_valid() {
        let left = vec![Value::Int(5), Value::Int(10), Value::Int(15)];
        let right = vec![Value::Int(2), Value::Int(3), Value::Int(5)];
        let result = sub(&left, &right).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Value::Int(3));
        assert_eq!(result[1], Value::Int(7));
        assert_eq!(result[2], Value::Int(10));
    }

    #[test]
    fn test_sub_empty() {
        let left: Vec<Value> = vec![];
        let right: Vec<Value> = vec![];
        let result = sub(&left, &right).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_sub_same_numbers() {
        let left = vec![Value::Int(5), Value::Int(5)];
        let right = vec![Value::Int(5), Value::Int(5)];
        let result = sub(&left, &right).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Value::Int(0));
        assert_eq!(result[1], Value::Int(0));
    }
    #[test]

    fn test_cmp_dist_empty_slices() {
        let result = cmp_dist::<i32>(&[], &[]);
        assert_eq!(result.unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_cmp_dist_equal_slices() {
        let left = vec![1, 2, 3];
        let right = vec![1, 2, 3];
        let result = cmp_dist(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_cmp_dist_less_than() {
        let left = vec![1, 2];
        let right = vec![1, 3];
        let result = cmp_dist(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Less);
    }

    #[test]
    fn test_cmp_dist_greater_than() {
        let left = vec![1, 4];
        let right = vec![1, 3];
        let result = cmp_dist(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Greater);
    }

    #[test]
    fn test_cmp_dist_with_floats() {
        let left = vec![1.0, 2.0];
        let right = vec![1.0, 2.0];
        let result = cmp_dist(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_cmp_dist_with_nan() {
        let left = vec![1.0, f64::NAN];
        let right = vec![1.0, 2.0];
        let result = cmp_dist(&left, &right);
        assert!(matches!(result, Err(Error::InvalidFormat(_))));
    }
    #[test]
    fn test_rectangle_contains_empty() {
        let container = Rectangle::<i32> {
            min: smallvec![],
            max: smallvec![],
        };
        let rect = Rectangle::<i32> {
            min: smallvec![1, 2],
            max: smallvec![3, 4],
        };
        assert!(!container.contains(&rect));
    }

    #[test]
    fn test_rectangle_contains_true() {
        let container = Rectangle::<i32> {
            min: smallvec![0, 0],
            max: smallvec![10, 10],
        };
        let rect = Rectangle::<i32> {
            min: smallvec![2, 2],
            max: smallvec![8, 8],
        };
        assert!(container.contains(&rect));
    }

    #[test]
    fn test_rectangle_contains_false_min_boundary() {
        let container = Rectangle::<i32> {
            min: smallvec![1, 1],
            max: smallvec![10, 10],
        };
        let rect = Rectangle::<i32> {
            min: smallvec![0, 5],
            max: smallvec![5, 8],
        };
        assert!(!container.contains(&rect));
    }

    #[test]
    fn test_rectangle_contains_false_max_boundary() {
        let container = Rectangle::<i32> {
            min: smallvec![0, 0],
            max: smallvec![10, 10],
        };
        let rect = Rectangle::<i32> {
            min: smallvec![5, 5],
            max: smallvec![11, 8],
        };
        assert!(!container.contains(&rect));
    }

    #[test]
    fn test_rectangle_contains_higher_dimensions() {
        let container = Rectangle::<i32> {
            min: smallvec![0, 0, 0],
            max: smallvec![10, 10, 10],
        };
        let rect = Rectangle::<i32> {
            min: smallvec![1, 1, 1],
            max: smallvec![9, 9, 9],
        };
        assert!(container.contains(&rect));
    }
}
