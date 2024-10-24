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
        let self_iter = self
            .max
            .iter()
            .zip(self.min.iter())
            .map(|(max, min)| max.try_sub(min));
        let other_iter = other
            .max
            .iter()
            .zip(other.min.iter())
            .map(|(max, min)| max.try_sub(min));
        for (own, other) in self_iter.zip(other_iter) {
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
    fn test_rectangle_cmp_with_priority_greater() {
        let larger = Rectangle::new(smallvec![0, 0], smallvec![10, 10]);
        let smaller = Rectangle::new(smallvec![1, 1], smallvec![8, 8]);
        assert_eq!(
            larger.cmp_with_priority(&smaller).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_rectangle_cmp_with_priority_less() {
        let larger = Rectangle::new(smallvec![0, 0], smallvec![10, 10]);
        let smaller = Rectangle::new(smallvec![1, 1], smallvec![8, 8]);
        assert_eq!(smaller.cmp_with_priority(&larger).unwrap(), Ordering::Less);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_equal() {
        let rect1 = Rectangle::new(smallvec![0, 0], smallvec![5, 5]);
        let rect2 = Rectangle::new(smallvec![0, 0], smallvec![5, 5]);
        assert_eq!(rect1.cmp_with_priority(&rect2).unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_partial_dimensions() {
        let rect1 = Rectangle::new(smallvec![0, 0], smallvec![6, 4]);
        let rect2 = Rectangle::new(smallvec![0, 0], smallvec![4, 6]);
        assert!(rect1.cmp_with_priority(&rect2).is_ok());
    }

    #[test]
    fn test_rectangle_cmp_with_priority_overlapping() {
        let rect1 = Rectangle::new(smallvec![2, 0], smallvec![8, 4]);
        let rect2 = Rectangle::new(smallvec![0, 2], smallvec![6, 6]);
        assert!(rect1.cmp_with_priority(&rect2).is_ok());
    }
    #[test]
    fn test_expand_with_node_smaller_values() {
        let mut bounds = Rectangle::new(smallvec![5, 5, 5], smallvec![10, 10, 10]);
        let node: SmallVec<[i32; 4]> = smallvec![3, 4, 2];
        bounds.expand_with_node(node);
        assert_eq!(bounds.min, smallvec![3, 4, 2] as SmallVec<[i32; 4]>);
        assert_eq!(bounds.max, smallvec![10, 10, 10] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_expand_with_node_larger_values() {
        let mut bounds = Rectangle::new(smallvec![5, 5, 5], smallvec![10, 10, 10]);
        let node: SmallVec<[i32; 4]> = smallvec![6, 12, 11];
        bounds.expand_with_node(node);
        assert_eq!(bounds.min, smallvec![5, 5, 5] as SmallVec<[i32; 4]>);
        assert_eq!(bounds.max, smallvec![10, 12, 11] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_expand_with_node_mixed_values() {
        let mut bounds = Rectangle::new(smallvec![5, 5, 5], smallvec![10, 10, 10]);
        let node: SmallVec<[i32; 4]> = smallvec![3, 15, 7];
        bounds.expand_with_node(node);
        assert_eq!(bounds.min, smallvec![3, 5, 5] as SmallVec<[i32; 4]>);
        assert_eq!(bounds.max, smallvec![10, 15, 10] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_expand_with_node_equal_values() {
        let mut bounds = Rectangle::new(smallvec![5, 5, 5], smallvec![10, 10, 10]);
        let node: SmallVec<[i32; 4]> = smallvec![5, 10, 7];
        bounds.expand_with_node(node);
        assert_eq!(bounds.min, smallvec![5, 5, 5] as SmallVec<[i32; 4]>);
        assert_eq!(bounds.max, smallvec![10, 10, 10] as SmallVec<[i32; 4]>);
    }
    #[test]
    fn test_rectangle_expand() {
        let mut rect1 = Rectangle {
            min: smallvec![0, 0],
            max: smallvec![5, 5],
        };

        let rect2 = Rectangle {
            min: smallvec![-1, -1],
            max: smallvec![3, 6],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![-1, -1] as SmallVec<[i32; 4]>);
        assert_eq!(rect1.max, smallvec![5, 6] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_rectangle_expand_no_change() {
        let mut rect1 = Rectangle {
            min: smallvec![0, 0],
            max: smallvec![10, 10],
        };

        let rect2 = Rectangle {
            min: smallvec![2, 2],
            max: smallvec![8, 8],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![0, 0] as SmallVec<[i32; 4]>);
        assert_eq!(rect1.max, smallvec![10, 10] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_rectangle_expand_single_dimension() {
        let mut rect1 = Rectangle {
            min: smallvec![5],
            max: smallvec![10],
        };

        let rect2 = Rectangle {
            min: smallvec![3],
            max: smallvec![12],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![3] as SmallVec<[i32; 4]>);
        assert_eq!(rect1.max, smallvec![12] as SmallVec<[i32; 4]>);
    }

    #[test]
    fn test_rectangle_expand_empty() {
        let mut rect1: Rectangle<i32> = Rectangle {
            min: smallvec![],
            max: smallvec![],
        };

        let rect2 = Rectangle {
            min: smallvec![],
            max: smallvec![],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![] as SmallVec<[i32; 4]>);
        assert_eq!(rect1.max, smallvec![] as SmallVec<[i32; 4]>);
    }
}
