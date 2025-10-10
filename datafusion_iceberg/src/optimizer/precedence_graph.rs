use std::collections::HashSet;

use iceberg_rust::error::Error as IcebergError;

use crate::{
    error::Error,
    optimizer::{
        cost::CostEstimator,
        query_graph::{NodeId, QueryGraph},
    },
};

struct NodeEstimates {
    node_id: NodeId,
    // T in [IbarakiKameda86]
    cardinality: usize,
    // C in [IbarakiKameda86]
    cost: f64,
}

struct PrecedenceTreeNode<'graph> {
    query_nodes: Vec<NodeEstimates>,
    children: Vec<PrecedenceTreeNode<'graph>>,
    query_graph: &'graph QueryGraph,
}

impl<'graph> PrecedenceTreeNode<'graph> {
    pub(crate) fn from_query_graph(
        graph: &'graph QueryGraph,
        root_id: NodeId,
    ) -> Result<Self, Error> {
        let mut remaining: HashSet<NodeId> = graph.nodes().map(|(x, _)| x).collect();
        remaining.remove(&root_id);
        PrecedenceTreeNode::from_query_node(root_id, 1.0, graph, &mut remaining)
    }

    fn from_query_node(
        node_id: NodeId,
        selectivity: f64,
        query_graph: &'graph QueryGraph,
        remaining: &mut HashSet<NodeId>,
    ) -> Result<Self, Error> {
        let node = query_graph
            .get_node(node_id)
            .ok_or(IcebergError::NotFound("Root node".to_owned()))?;
        let input_cardinality = Self::cardinality(&node.data).unwrap_or(1);
        let children = node
            .connections()
            .iter()
            .filter_map(|edge| {
                if let Some(edge) = query_graph.get_edge(*edge) {
                    if let Some(other) = edge
                        .nodes
                        .into_iter()
                        .find(|x| *x != node_id && remaining.contains(x))
                    {
                        remaining.remove(&other);
                        let selectivity = Self::selectivity(&edge.data);
                        Some(PrecedenceTreeNode::from_query_node(
                            other,
                            selectivity,
                            query_graph,
                            remaining,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(PrecedenceTreeNode {
            query_nodes: vec![NodeEstimates {
                node_id,
                cardinality: (selectivity * input_cardinality as f64) as usize,
                cost: Self::cost(selectivity, input_cardinality),
            }],
            children,
            query_graph,
        })
    }

    fn rank(&self) -> f64 {
        let (cardinality, cost) =
            self.query_nodes
                .iter()
                .fold((0, 0.0), |(cardinality, cost), node| {
                    let cost = cost + cardinality as f64 * node.cost;
                    let cardinality = cardinality * node.cardinality;
                    (cardinality, cost)
                });
        (cardinality - 1) as f64 / cost
    }

    fn normalize(&mut self) {
        match self.children.len() {
            0 => (),
            1 => {
                if self.children[0].rank() < self.rank() {
                    // Create normalized node as a sequence with child node
                    let mut child = self.children.pop().unwrap();
                    self.query_nodes.append(&mut child.query_nodes);
                    self.children = child.children;
                    self.normalize();
                } else {
                    self.children[0].normalize();
                }
            }
            _ => {
                for child in &mut self.children {
                    // Normalize child trees into chains
                    child.normalize();
                }
                let mut children = std::mem::take(&mut self.children).into_iter();
                // Merge child chains into single chain
                let left = children.next().unwrap();
                let child = children.fold(left, Self::merge);

                self.children = vec![child]
            }
        }
    }

    fn merge(self, other: PrecedenceTreeNode<'graph>) -> Self {
        let (mut first, second) = if self.rank() < other.rank() {
            (self, other)
        } else {
            (other, self)
        };
        if first.children.is_empty() {
            first.children = vec![second];
        } else {
            first.children = vec![first.children.pop().unwrap().merge(second)];
        }
        first
    }

    fn denormalize(&mut self) -> Result<(), Error> {
        if self.children.len() != 1 {
            return Err(IcebergError::InvalidFormat("Not normalized tree".to_owned()).into());
        }
        if self.query_nodes.len() == 1 {
            self.children[0].denormalize()
        } else {
            todo!();
            Ok(())
        }
    }
}

impl<'graph> CostEstimator for PrecedenceTreeNode<'graph> {}
