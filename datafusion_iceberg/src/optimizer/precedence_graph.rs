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

    /// Rank function according to IbarakiKameda86
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

    /// Normalizes the precedence tree into a linear chain structure.
    ///
    /// This transformation converts the tree into a normalized form where each node
    /// has at most one child, creating a linear sequence of query nodes. The normalization
    /// process uses the rank function to determine optimal ordering according to the
    /// Ibaraki-Kameda algorithm.
    ///
    /// The normalization handles three cases:
    /// - **Leaf nodes (0 children)**: Already normalized, no action needed
    /// - **Single child (1 child)**: If the child has lower rank than current node, merge
    ///   the child's query nodes into the current node, creating a sequence. Otherwise,
    ///   recursively normalize the child.
    /// - **Multiple children (2+ children)**: Recursively normalize all children into chains,
    ///   then merge all child chains into a single chain using the merge operation.
    ///
    /// After normalization, the tree becomes a chain where nodes are ordered by their
    /// rank values, with each node containing one or more query operations in sequence.
    ///
    /// # Algorithm
    ///
    /// Based on the Ibaraki-Kameda join ordering algorithm, which optimizes query
    /// execution by arranging operations to minimize intermediate result sizes.
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

    /// Merges two precedence tree chains into a single chain.
    ///
    /// This operation combines two normalized tree chains (each with at most one child)
    /// into a single chain, preserving rank ordering. The chain with the lower rank becomes
    /// the parent, and the higher-ranked chain is attached as a descendant.
    ///
    /// The merge strategy depends on whether the lower-ranked chain has children:
    /// - **No children**: The higher-ranked chain becomes the direct child
    /// - **Has child**: Recursively merge the higher-ranked chain with the child,
    ///   maintaining the chain structure
    ///
    /// This ensures the resulting chain maintains proper rank ordering from root to leaf,
    /// which is essential for the Ibaraki-Kameda optimization algorithm.
    ///
    /// # Arguments
    ///
    /// * `self` - The first tree chain to merge
    /// * `other` - The second tree chain to merge
    ///
    /// # Returns
    ///
    /// Returns a merged `PrecedenceTreeNode` chain with both input chains combined,
    /// ordered by rank values.
    ///
    /// # Panics
    ///
    /// May panic if called on non-normalized trees (trees with multiple children).
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
