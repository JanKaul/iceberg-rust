use std::collections::HashSet;

use datafusion_expr::LogicalPlan;
use iceberg_rust::error::Error as IcebergError;

use crate::{
    error::Error,
    optimizer::{
        cost::CostEstimator,
        query_graph::{NodeId, QueryGraph},
    },
};

/// Generates an optimized linear join plan from a query graph using the Ibaraki-Kameda algorithm.
///
/// This function finds the optimal join ordering for a query by:
/// 1. Trying each node in the query graph as a potential root
/// 2. For each root, building a precedence tree and optimizing it through normalization/denormalization
/// 3. Selecting the plan with the lowest estimated cost
///
/// The optimization process uses the Ibaraki-Kameda algorithm, which arranges joins to minimize
/// intermediate result sizes by considering both cardinality and cost estimates.
///
/// # Algorithm Steps
///
/// For each candidate root node:
/// 1. **Construction**: Build a precedence tree from the query graph starting at that node
/// 2. **Normalization**: Transform the tree into a chain structure ordered by rank
/// 3. **Denormalization**: Split merged operations back into individual nodes while maintaining chain structure
/// 4. **Cost Comparison**: Compare the resulting plan's cost against the current best
///
/// # Arguments
///
/// * `query_graph` - The query graph containing logical plan nodes and join specifications
///
/// # Returns
///
/// Returns a `LogicalPlan` representing the optimal join ordering with the lowest estimated cost.
///
/// # Errors
///
/// Returns an error if:
/// - The query graph is empty or invalid
/// - Tree construction, normalization, or denormalization fails
/// - No valid precedence graph can be generated
pub(crate) fn linearized_join_plan(query_graph: QueryGraph) -> Result<LogicalPlan, Error> {
    let mut best_graph: Option<PrecedenceTreeNode> = None;

    for (node_id, _) in query_graph.nodes() {
        let mut precedence_graph = PrecedenceTreeNode::from_query_graph(&query_graph, node_id)?;
        precedence_graph.normalize();
        precedence_graph.denormalize()?;

        best_graph = match best_graph.take() {
            Some(current) => {
                let new_cost = precedence_graph.query_nodes[0].cost;
                if new_cost < current.query_nodes[0].cost {
                    Some(precedence_graph)
                } else {
                    Some(current)
                }
            }
            None => Some(precedence_graph),
        };
    }

    best_graph
        .ok_or(IcebergError::InvalidFormat(
            "No valid precedence graph found".to_owned(),
        ))?
        .into_logical_plan(&query_graph)
}

struct JoinNode {
    node_id: NodeId,
    // T in [IbarakiKameda86]
    cardinality: usize,
    // C in [IbarakiKameda86]
    cost: f64,
}

impl JoinNode {
    fn rank(&self) -> f64 {
        (self.cardinality - 1) as f64 / self.cost
    }
}

/// A node in the precedence tree for query optimization.
///
/// The precedence tree is a data structure used by the Ibaraki-Kameda algorithm for
/// optimizing join ordering in database queries. It can represent both arbitrary tree
/// structures and linear chain structures (where each node has at most one child).
///
/// # Lifecycle
///
/// A typical precedence tree goes through three phases:
///
/// 1. **Construction** ([`from_query_graph`](Self::from_query_graph)): Build an initial tree
///    from a query graph, creating nodes with cost/cardinality estimates
/// 2. **Normalization** ([`normalize`](Self::normalize)): Transform the tree into a chain
///    where nodes are ordered by rank, potentially merging multiple query operations into
///    single nodes
/// 3. **Denormalization** ([`denormalize`](Self::denormalize)): Split merged operations back
///    into individual nodes while maintaining the optimized chain structure
///
/// The result is a linear execution order that minimizes intermediate result sizes.
///
/// # Fields
///
/// * `query_nodes` - Vector of query operations with cost estimates. In an initial tree,
///   contains one operation. After normalization, may contain multiple merged operations.
///   After denormalization, contains exactly one operation.
/// * `children` - Child nodes in the tree. In a normalized/denormalized chain, contains
///   at most one child. In an arbitrary tree, may contain multiple children.
/// * `query_graph` - Reference to the original query graph, used for accessing node
///   relationships and metadata during tree transformations.
struct PrecedenceTreeNode<'graph> {
    query_nodes: Vec<JoinNode>,
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
            query_nodes: vec![JoinNode {
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

    /// Denormalizes a normalized precedence tree by splitting merged query nodes.
    ///
    /// This is the inverse operation of normalization, but with a critical property:
    /// **the result is still a chain structure** (each node has at most one child).
    /// It converts a normalized chain where nodes contain multiple query operations
    /// into a longer chain where each node contains exactly one query operation.
    ///
    /// The denormalization process:
    /// 1. **Validates input**: Ensures the tree is normalized (0 or 1 children per node)
    /// 2. **Recursively processes children**: Denormalizes the child chain first
    /// 3. **Splits merged nodes**: For nodes with multiple query operations, iteratively
    ///    extracts operations one at a time based on neighbor relationships with the child
    /// 4. **Maintains ordering**: Uses rank-based selection to determine which query node
    ///    to extract next, choosing the highest-ranked neighbor of the child node
    ///
    /// **Key property**: After denormalization, the result remains a chain (not a tree with
    /// branches). Each node contains exactly one query operation, but the chain structure
    /// is preserved. This is the essence of the normalize-denormalize algorithm: transforming
    /// an arbitrary tree into an optimized chain while respecting query dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The tree is not normalized (has more than one child at any level)
    ///
    /// # Algorithm
    ///
    /// The splitting process uses the query graph's neighbor relationships to determine
    /// which nodes should be adjacent in the chain, maintaining logical dependencies
    /// between query operations while producing a linear execution order.
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

    /// Converts the precedence tree chain into a DataFusion `LogicalPlan`.
    ///
    /// This method walks down the optimized chain structure, building a left-deep join tree
    /// by repeatedly joining the accumulated result with the next node in the chain.
    ///
    /// # Algorithm
    ///
    /// 1. Start with the first node's `LogicalPlan` from the query graph
    /// 2. For each subsequent node in the chain:
    ///    - Get the node's `LogicalPlan` from the query graph
    ///    - Find the edge connecting the current and next nodes
    ///    - Create a join using the edge's join specification
    ///    - The accumulated plan becomes the left side of the join
    /// 3. Return the final joined `LogicalPlan`
    ///
    /// # Arguments
    ///
    /// * `query_graph` - The query graph containing the logical plans and join specifications
    ///
    /// # Returns
    ///
    /// Returns a `LogicalPlan` representing the optimized join execution order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A node or edge is missing from the query graph
    /// - The precedence tree is not in the expected chain format
    pub(crate) fn into_logical_plan(self, query_graph: &QueryGraph) -> Result<LogicalPlan, Error> {
        // Get the first node's logical plan
        let mut current_node_id = self.query_nodes[0].node_id;
        let mut current_plan = query_graph
            .get_node(current_node_id)
            .ok_or(IcebergError::NotFound(format!(
                "Node {:?}",
                current_node_id
            )))?
            .data
            .as_ref()
            .clone();

        // Walk down the chain, joining each subsequent node
        let mut current_chain = &self;

        while !current_chain.children.is_empty() {
            let child = &current_chain.children[0];
            let next_node_id = child.query_nodes[0].node_id;

            // Get the next node's logical plan
            let next_plan = query_graph
                .get_node(next_node_id)
                .ok_or(IcebergError::NotFound(format!("Node {:?}", next_node_id)))?
                .data
                .as_ref()
                .clone();

            // Find the edge connecting current and next nodes
            let current_node =
                query_graph
                    .get_node(current_node_id)
                    .ok_or(IcebergError::NotFound(format!(
                        "Node {:?}",
                        current_node_id
                    )))?;

            let edge = current_node
                .connection_with(next_node_id, query_graph)
                .ok_or(IcebergError::NotFound(format!(
                    "Edge between {:?} and {:?}",
                    current_node_id, next_node_id
                )))?;

            // Create the join plan
            current_plan = LogicalPlan::Join(datafusion_expr::Join {
                left: std::sync::Arc::new(current_plan),
                right: std::sync::Arc::new(next_plan),
                on: edge.data.on.clone(),
                filter: edge.data.filter.clone(),
                join_type: edge.data.join_type,
                join_constraint: edge.data.join_constraint,
                schema: edge.data.schema.clone(),
                null_equality: edge.data.null_equality,
            });

            // Move to the next node in the chain
            current_node_id = next_node_id;
            current_chain = child;
        }

        Ok(current_plan)
    }
}

impl<'graph> CostEstimator for PrecedenceTreeNode<'graph> {}
