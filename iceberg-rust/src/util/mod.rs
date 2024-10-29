use std::cmp::Ordering;

use iceberg_rust_spec::{
    manifest_list::FieldSummary,
    values::{Struct, TrySub, Value},
};
use smallvec::SmallVec;

use crate::error::Error;

type Vec4<T> = SmallVec<[T; 4]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rectangle {
    pub min: Vec4<Value>,
    pub max: Vec4<Value>,
}

impl Rectangle {
    pub(crate) fn new(min: Vec4<Value>, max: Vec4<Value>) -> Self {
        Self { min, max }
    }

    /// Expands the rectangle to include the given rectangle.
    ///
    /// This method updates the minimum and maximum values of the rectangle to include
    /// the values in the given `rect` rectangle.
    pub(crate) fn expand(&mut self, rect: &Rectangle) {
        for i in 0..self.min.len() {
            if rect.min[i] < self.min[i] {
                self.min[i] = rect.min[i].clone();
            }
            if rect.max[i] > self.max[i] {
                self.max[i] = rect.max[i].clone();
            }
        }
    }

    /// Expands the rectangle to include the given node.
    ///
    /// This method updates the minimum and maximum values of the rectangle to include
    /// the values in the given `node` vector.
    pub(crate) fn expand_with_node(&mut self, node: Vec4<Value>) {
        for i in 0..self.min.len() {
            if node[i] < self.min[i] {
                self.min[i] = node[i].clone();
            }
            if node[i] > self.max[i] {
                self.max[i] = node[i].clone();
            }
        }
    }

    /// Determine if one rectangle is larger than the other.
    ///
    ///Values the earlier columns more than the later.
    pub(crate) fn cmp_with_priority(&self, other: &Rectangle) -> Result<Ordering, Error> {
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

/// Converts the values of a partition struct into a vector in the order that the columns appear in the partition spec
pub(crate) fn partition_struct_to_vec(
    partition_struct: &Struct,
    names: &[&str],
) -> Result<Vec4<Value>, Error> {
    names
        .iter()
        .map(|x| partition_struct.get(x).and_then(Clone::clone))
        .collect::<Option<SmallVec<_>>>()
        .ok_or(Error::InvalidFormat("Partition struct".to_owned()))
}

pub(crate) fn summary_to_rectangle(summaries: &[FieldSummary]) -> Result<Rectangle, Error> {
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

/// Compares two vectors by giving a higher priority to the earlier dimensions compared to later dimensions
pub(crate) fn cmp_with_priority(left: &[Value], right: &[Value]) -> Result<Ordering, Error> {
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

/// Try to subtract a value vector from another
pub(crate) fn try_sub(left: &[Value], right: &[Value]) -> Result<Vec4<Value>, Error> {
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
        let result = try_sub(&left, &right).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Value::Int(3));
        assert_eq!(result[1], Value::Int(7));
        assert_eq!(result[2], Value::Int(10));
    }

    #[test]
    fn test_sub_empty() {
        let left: Vec<Value> = vec![];
        let right: Vec<Value> = vec![];
        let result = try_sub(&left, &right).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_sub_same_numbers() {
        let left = vec![Value::Int(5), Value::Int(5)];
        let right = vec![Value::Int(5), Value::Int(5)];
        let result = try_sub(&left, &right).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Value::Int(0));
        assert_eq!(result[1], Value::Int(0));
    }
    #[test]

    fn test_cmp_dist_empty_slices() {
        let result = cmp_with_priority(&[], &[]);
        assert_eq!(result.unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_cmp_dist_equal_slices() {
        let left = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let right = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = cmp_with_priority(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_cmp_dist_less_than() {
        let left = vec![Value::Int(1), Value::Int(2)];
        let right = vec![Value::Int(1), Value::Int(3)];
        let result = cmp_with_priority(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Less);
    }

    #[test]
    fn test_cmp_dist_greater_than() {
        let left = vec![Value::Int(1), Value::Int(4)];
        let right = vec![Value::Int(1), Value::Int(3)];
        let result = cmp_with_priority(&left, &right);
        assert_eq!(result.unwrap(), Ordering::Greater);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_greater() {
        let larger = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(10), Value::Int(10)],
        );
        let smaller = Rectangle::new(
            smallvec![Value::Int(1), Value::Int(1)],
            smallvec![Value::Int(8), Value::Int(8)],
        );
        assert_eq!(
            larger.cmp_with_priority(&smaller).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_rectangle_cmp_with_priority_less() {
        let larger = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(10), Value::Int(10)],
        );
        let smaller = Rectangle::new(
            smallvec![Value::Int(1), Value::Int(1)],
            smallvec![Value::Int(8), Value::Int(8)],
        );
        assert_eq!(smaller.cmp_with_priority(&larger).unwrap(), Ordering::Less);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_equal() {
        let rect1 = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(5), Value::Int(5)],
        );
        let rect2 = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(5), Value::Int(5)],
        );
        assert_eq!(rect1.cmp_with_priority(&rect2).unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_partial_dimensions() {
        let rect1 = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(6), Value::Int(4)],
        );
        let rect2 = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(0)],
            smallvec![Value::Int(4), Value::Int(6)],
        );
        assert_eq!(rect1.cmp_with_priority(&rect2).unwrap(), Ordering::Greater);
    }

    #[test]
    fn test_rectangle_cmp_with_priority_overlapping() {
        let rect1 = Rectangle::new(
            smallvec![Value::Int(2), Value::Int(0)],
            smallvec![Value::Int(8), Value::Int(4)],
        );
        let rect2 = Rectangle::new(
            smallvec![Value::Int(0), Value::Int(2)],
            smallvec![Value::Int(6), Value::Int(6)],
        );
        assert_eq!(rect1.cmp_with_priority(&rect2).unwrap(), Ordering::Equal);
    }
    #[test]
    fn test_expand_with_node_smaller_values() {
        let mut bounds = Rectangle::new(
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)],
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)],
        );
        let node: SmallVec<[Value; 4]> = smallvec![Value::Int(3), Value::Int(4), Value::Int(2)];
        bounds.expand_with_node(node);
        assert_eq!(
            bounds.min,
            smallvec![Value::Int(3), Value::Int(4), Value::Int(2)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            bounds.max,
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)] as SmallVec<[Value; 4]>
        );
    }

    #[test]
    fn test_expand_with_node_larger_values() {
        let mut bounds = Rectangle::new(
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)],
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)],
        );
        let node: SmallVec<[Value; 4]> = smallvec![Value::Int(6), Value::Int(12), Value::Int(11)];
        bounds.expand_with_node(node);
        assert_eq!(
            bounds.min,
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            bounds.max,
            smallvec![Value::Int(10), Value::Int(12), Value::Int(11)] as SmallVec<[Value; 4]>
        );
    }

    #[test]
    fn test_expand_with_node_mixed_values() {
        let mut bounds = Rectangle::new(
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)],
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)],
        );
        let node: SmallVec<[Value; 4]> = smallvec![Value::Int(3), Value::Int(15), Value::Int(7)];
        bounds.expand_with_node(node);
        assert_eq!(
            bounds.min,
            smallvec![Value::Int(3), Value::Int(5), Value::Int(5)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            bounds.max,
            smallvec![Value::Int(10), Value::Int(15), Value::Int(10)] as SmallVec<[Value; 4]>
        );
    }

    #[test]
    fn test_expand_with_node_equal_values() {
        let mut bounds = Rectangle::new(
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)],
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)],
        );
        let node: SmallVec<[Value; 4]> = smallvec![Value::Int(5), Value::Int(10), Value::Int(7)];
        bounds.expand_with_node(node);
        assert_eq!(
            bounds.min,
            smallvec![Value::Int(5), Value::Int(5), Value::Int(5)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            bounds.max,
            smallvec![Value::Int(10), Value::Int(10), Value::Int(10)] as SmallVec<[Value; 4]>
        );
    }
    #[test]
    fn test_rectangle_expand() {
        let mut rect1 = Rectangle {
            min: smallvec![Value::Int(0), Value::Int(0)],
            max: smallvec![Value::Int(5), Value::Int(5)],
        };

        let rect2 = Rectangle {
            min: smallvec![Value::Int(-1), Value::Int(-1)],
            max: smallvec![Value::Int(3), Value::Int(6)],
        };

        rect1.expand(&rect2);

        assert_eq!(
            rect1.min,
            smallvec![Value::Int(-1), Value::Int(-1)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            rect1.max,
            smallvec![Value::Int(5), Value::Int(6)] as SmallVec<[Value; 4]>
        );
    }

    #[test]
    fn test_rectangle_expand_no_change() {
        let mut rect1 = Rectangle {
            min: smallvec![Value::Int(0), Value::Int(0)],
            max: smallvec![Value::Int(10), Value::Int(10)],
        };

        let rect2 = Rectangle {
            min: smallvec![Value::Int(2), Value::Int(2)],
            max: smallvec![Value::Int(8), Value::Int(8)],
        };

        rect1.expand(&rect2);

        assert_eq!(
            rect1.min,
            smallvec![Value::Int(0), Value::Int(0)] as SmallVec<[Value; 4]>
        );
        assert_eq!(
            rect1.max,
            smallvec![Value::Int(10), Value::Int(10)] as SmallVec<[Value; 4]>
        );
    }

    #[test]
    fn test_rectangle_expand_single_dimension() {
        let mut rect1 = Rectangle {
            min: smallvec![Value::Int(5)],
            max: smallvec![Value::Int(10)],
        };

        let rect2 = Rectangle {
            min: smallvec![Value::Int(3)],
            max: smallvec![Value::Int(12)],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![Value::Int(3)] as SmallVec<[Value; 4]>);
        assert_eq!(rect1.max, smallvec![Value::Int(12)] as SmallVec<[Value; 4]>);
    }

    #[test]
    fn test_rectangle_expand_empty() {
        let mut rect1: Rectangle = Rectangle {
            min: smallvec![],
            max: smallvec![],
        };

        let rect2 = Rectangle {
            min: smallvec![],
            max: smallvec![],
        };

        rect1.expand(&rect2);

        assert_eq!(rect1.min, smallvec![] as SmallVec<[Value; 4]>);
        assert_eq!(rect1.max, smallvec![] as SmallVec<[Value; 4]>);
    }
}
