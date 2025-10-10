use std::collections::HashSet;

use iceberg_rust::error::Error as IcebergError;

use crate::{
    error::Error,
    optimizer::{
        cardinality::CardinalityEstimate,
        query_graph::{NodeId, QueryGraph},
        selectivity::SelectivityEstimate,
    },
};

struct PrecedenceNode<'graph> {
    query_nodes: Vec<NodeId>,
    children: Vec<PrecedenceNode<'graph>>,
    cardinality: usize,
    selectivity: f64,
    query_graph: &'graph QueryGraph,
}

impl<'graph> PrecedenceNode<'graph> {
    pub(crate) fn from_query_graph(
        graph: &'graph QueryGraph,
        root_id: NodeId,
    ) -> Result<Self, Error> {
        let mut remaining: HashSet<NodeId> = graph.nodes().map(|(x, _)| x).collect();
        remaining.remove(&root_id);
        PrecedenceNode::from_query_node(root_id, 1.0, graph, &mut remaining)
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
        let connections = node.connections();
        let children = connections
            .iter()
            .filter_map(|edge| {
                if let Some(edge) = query_graph.get_edge(*edge) {
                    if let Some(other) = edge
                        .nodes
                        .into_iter()
                        .find(|x| *x != node_id && remaining.contains(x))
                    {
                        remaining.remove(&other);
                        Some(PrecedenceNode::from_query_node(
                            other,
                            Self::selectivity(&edge.data),
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
        Ok(PrecedenceNode {
            query_nodes: vec![node_id],
            children,
            cardinality: Self::cardinality(&node.data).unwrap_or(1),
            selectivity,
            query_graph,
        })
    }
}

impl<'graph> SelectivityEstimate for PrecedenceNode<'graph> {}
impl<'graph> CardinalityEstimate for PrecedenceNode<'graph> {}
