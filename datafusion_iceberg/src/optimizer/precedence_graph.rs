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

impl NodeEstimates {
    fn rank(&self) -> f64 {
        (self.cardinality - 1) as f64 / self.cost
    }
}

struct PrecedenceTreeNode<'graph> {
    query_nodes: Vec<NodeEstimates>,
    children: Vec<PrecedenceTreeNode<'graph>>,
    query_graph: &'graph QueryGraph,
}

impl<'graph> PrecedenceTreeNode<'graph> {
    /// Creates a precedence tree from a query graph.
    ///
    /// This is the main entry point for transforming a query graph into a precedence tree
    /// structure. The tree represents an initial join ordering with cost and cardinality
    /// estimates for query optimization.
    ///
    /// The function performs a depth-first traversal starting from the root node,
    /// building a tree where:
    /// - Each node contains cost/cardinality estimates for a query operation
    /// - Children represent connected query nodes (joins, filters, etc.)
    /// - The root node starts with selectivity of 1.0 (no filtering)
    ///
    /// # Arguments
    ///
    /// * `graph` - The query graph to transform into a precedence tree
    /// * `root_id` - The ID of the node to use as the root of the tree
    ///
    /// # Returns
    ///
    /// Returns a `PrecedenceTreeNode` representing the entire query graph as a tree structure,
    /// with the specified root node at the top.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `root_id` is not found in the query graph
    /// - Any connected node cannot be found during traversal
    pub(crate) fn from_query_graph(
        graph: &'graph QueryGraph,
        root_id: NodeId,
    ) -> Result<Self, Error> {
        let mut remaining: HashSet<NodeId> = graph.nodes().map(|(x, _)| x).collect();
        remaining.remove(&root_id);
        PrecedenceTreeNode::from_query_node(root_id, 1.0, graph, &mut remaining)
    }

    /// Recursively constructs a precedence tree node from a query graph node.
    ///
    /// This function builds a tree structure by:
    /// 1. Creating a node with cost and cardinality estimates for the current query node
    /// 2. Recursively processing all connected unvisited nodes as children
    /// 3. Removing visited nodes from the `remaining` set to avoid cycles
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the query graph node to process
    /// * `selectivity` - The selectivity factor from the parent edge (1.0 for root)
    /// * `query_graph` - Reference to the query graph being transformed
    /// * `remaining` - Mutable set of node IDs not yet visited (updated during traversal)
    ///
    /// # Returns
    ///
    /// Returns a `PrecedenceTreeNode` containing:
    /// - A single `NodeEstimates` with cardinality and cost based on input cardinality and selectivity
    /// - Child nodes for each connected unvisited neighbor in the query graph
    ///
    /// # Errors
    ///
    /// Returns an error if the specified `node_id` is not found in the query graph.
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
            .filter_map(|edge_id| {
                let edge = query_graph.get_edge(*edge_id)?;
                let other = edge
                    .nodes
                    .into_iter()
                    .find(|x| *x != node_id && remaining.contains(x))?;

                remaining.remove(&other);
                let child_selectivity = Self::selectivity(&edge.data);
                Some(PrecedenceTreeNode::from_query_node(
                    other,
                    child_selectivity,
                    query_graph,
                    remaining,
                ))
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
                // If child has lower rank, merge it into current node
                if self.children[0].rank() < self.rank() {
                    let mut child = self.children.pop().unwrap();
                    self.query_nodes.append(&mut child.query_nodes);
                    self.children = child.children;
                    self.normalize();
                } else {
                    self.children[0].normalize();
                }
            }
            _ => {
                // Normalize all child trees into chains, then merge them
                for child in &mut self.children {
                    child.normalize();
                }
                let child = std::mem::take(&mut self.children)
                    .into_iter()
                    .reduce(Self::merge)
                    .unwrap();
                self.children = vec![child];
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
        // Normalized trees must have 0 or 1 children
        match self.children.len() {
            0 => return Ok(()),
            1 => self.children[0].denormalize()?,
            _ => return Err(IcebergError::InvalidFormat("Not normalized tree".to_owned()).into()),
        }

        // Split query nodes into a chain based on neighbor relationships
        while self.query_nodes.len() > 1 {
            let child_id = self.children[0].query_nodes[0].node_id;
            let child_node = self.query_graph.get_node(child_id).unwrap();
            let neighbours = child_node.neighbours(child_id, self.query_graph);

            // Find the highest-ranked neighbor node
            let highest_rank_idx = self
                .query_nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| neighbours.contains(&node.node_id))
                .max_by(|(_, a), (_, b)| a.rank().partial_cmp(&b.rank()).unwrap())
                .map(|(idx, _)| idx)
                .unwrap();

            let node = self.query_nodes.remove(highest_rank_idx);

            // Insert the node between current and its child
            let child = std::mem::replace(
                &mut self.children[0],
                PrecedenceTreeNode {
                    query_nodes: vec![node],
                    children: Vec::new(),
                    query_graph: self.query_graph,
                },
            );
            self.children[0].children = vec![child];
        }
        Ok(())
    }
}

impl<'graph> CostEstimator for PrecedenceTreeNode<'graph> {}
