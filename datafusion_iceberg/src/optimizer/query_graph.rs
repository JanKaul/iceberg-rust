use std::sync::Arc;

use datafusion_expr::{Join, LogicalPlan};

pub type NodeId = usize;
pub type EdgeId = usize;

pub struct Node<T> {
    pub data: T,
    connections: Vec<EdgeId>,
}

impl<N> Node<N> {
    pub(crate) fn connections(&self) -> &[EdgeId] {
        &self.connections
    }

    pub(crate) fn connection_with<'graph, E>(
        &self,
        node_id: NodeId,
        query_graph: &'graph UndirectedGraph<N, E>,
    ) -> Option<&'graph Edge<E>> {
        self.connections
            .iter()
            .filter_map(|edge_id| query_graph.get_edge(*edge_id))
            .find(move |x| x.nodes.contains(&node_id))
    }

    pub(crate) fn neighbours<E>(
        &self,
        node_id: NodeId,
        query_graph: &UndirectedGraph<N, E>,
    ) -> Vec<NodeId> {
        self.connections
            .iter()
            .filter_map(|edge_id| query_graph.get_edge(*edge_id))
            .flat_map(|edge| edge.nodes)
            .filter(|&id| id != node_id)
            .collect()
    }
}

pub type QueryNode = Node<Arc<LogicalPlan>>;

pub struct Edge<T> {
    pub nodes: [NodeId; 2],
    pub data: T,
}

pub type QueryEdge = Edge<Join>;

pub struct UndirectedGraph<N, E> {
    nodes: VecMap<Node<N>>,
    edges: VecMap<Edge<E>>,
}

pub type QueryGraph = UndirectedGraph<Arc<LogicalPlan>, Join>;

impl<N, E> UndirectedGraph<N, E> {
    pub(crate) fn new() -> Self {
        Self {
            nodes: VecMap::new(),
            edges: VecMap::new(),
        }
    }

    pub(crate) fn add_node(&mut self, other: NodeId, node_data: N, edge_data: E) -> Option<NodeId> {
        if self.nodes.contains_key(other) {
            let new_id = self.nodes.insert(Node {
                data: node_data,
                connections: Vec::new(),
            });
            self.add_edge(new_id, other, edge_data);
            Some(new_id)
        } else {
            None
        }
    }

    fn add_edge(&mut self, from: NodeId, to: NodeId, data: E) -> Option<EdgeId> {
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            let edge_id = self.edges.insert(Edge {
                nodes: [from, to],
                data,
            });
            if let Some(from) = self.nodes.get_mut(from) {
                from.connections.push(edge_id);
            }
            if let Some(to) = self.nodes.get_mut(to) {
                to.connections.push(edge_id);
            }
            Some(edge_id)
        } else {
            None
        }
    }

    pub(crate) fn remove_node(&mut self, node_id: NodeId) -> Option<N> {
        if let Some(node) = self.nodes.remove(node_id) {
            // Remove all edges connected to this node
            for edge_id in &node.connections {
                if let Some(edge) = self.edges.remove(*edge_id) {
                    // Remove the edge from the other node's connections
                    for other_node_id in edge.nodes {
                        if other_node_id != node_id {
                            if let Some(other_node) = self.nodes.get_mut(other_node_id) {
                                other_node.connections.retain(|id| id != edge_id);
                            }
                        }
                    }
                }
            }
            Some(node.data)
        } else {
            None
        }
    }

    fn remove_edge(&mut self, edge_id: EdgeId) -> Option<E> {
        if let Some(edge) = self.edges.remove(edge_id) {
            // Remove the edge from both nodes' connections
            for node_id in edge.nodes {
                if let Some(node) = self.nodes.get_mut(node_id) {
                    node.connections.retain(|id| *id != edge_id);
                }
            }
            Some(edge.data)
        } else {
            None
        }
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (NodeId, &Node<N>)> {
        self.nodes.iter()
    }

    pub(crate) fn get_node(&self, key: NodeId) -> Option<&Node<N>> {
        self.nodes.get(key)
    }

    pub(crate) fn get_edge(&self, key: EdgeId) -> Option<&Edge<E>> {
        self.edges.get(key)
    }
}

/// A simple Vec-based map that uses Option<T> for sparse storage
/// Keys are never reused once removed
struct VecMap<V>(Vec<Option<V>>);

impl<V> VecMap<V> {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn insert(&mut self, value: V) -> usize {
        let idx = self.0.len();
        self.0.push(Some(value));
        idx
    }

    fn get(&self, key: usize) -> Option<&V> {
        self.0.get(key)?.as_ref()
    }

    fn get_mut(&mut self, key: usize) -> Option<&mut V> {
        self.0.get_mut(key)?.as_mut()
    }

    fn remove(&mut self, key: usize) -> Option<V> {
        self.0.get_mut(key)?.take()
    }

    fn contains_key(&self, key: usize) -> bool {
        self.0.get(key).and_then(|v| v.as_ref()).is_some()
    }

    fn iter(&self) -> impl Iterator<Item = (usize, &V)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.as_ref().map(|v| (idx, v)))
    }
}
