use std::collections::HashSet;

use iceberg_rust::error::Error as IcebergError;

use crate::{
    error::Error,
    optimizer::query_graph::{NodeId, QueryGraph},
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
        PrecedenceNode::from_query_node(root_id, graph, &mut remaining)
    }

    fn from_query_node(
        node_id: NodeId,
        query_graph: &'graph QueryGraph,
        remaining: &mut HashSet<NodeId>,
    ) -> Result<Self, Error> {
        let node = query_graph
            .get_node(node_id)
            .ok_or(IcebergError::NotFound("Root node".to_owned()))?;
        let children = node
            .neighbours(node_id, query_graph)
            .into_iter()
            .filter_map(|x| {
                if remaining.contains(&x) {
                    remaining.remove(&x);
                    Some(PrecedenceNode::from_query_node(x, query_graph, remaining))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(PrecedenceNode {
            query_nodes: vec![node_id],
            children,
            cardinality: 0,
            selectivity: 1.0,
            query_graph,
        })
    }
}
