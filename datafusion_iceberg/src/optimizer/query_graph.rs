use slotmap::{new_key_type, SlotMap};

new_key_type! {
    pub struct NodeId;
}

struct Node<T> {
    data: T,
    connections: Vec<EdgeId>,
}

new_key_type! {
    pub struct EdgeId;
}

struct Edge<T> {
    nodes: [NodeId; 2],
    data: T,
}

struct QueryGraph<N, E> {
    nodes: SlotMap<NodeId, Node<N>>,
    edges: SlotMap<EdgeId, Edge<E>>,
}

impl<N, E> QueryGraph<N, E> {
    fn add_node(&mut self, data: N) -> NodeId {
        self.nodes.insert(Node {
            data,
            connections: Vec::new(),
        })
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
}
