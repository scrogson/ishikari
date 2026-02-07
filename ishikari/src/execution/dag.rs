//! DAG computation and execution level calculation.
//!
//! Computes execution levels from a workflow's dependency graph using
//! topological sort. Nodes at the same level can execute in parallel.

use std::collections::{HashMap, VecDeque};

use super::types::ExecutableWorkflow;

/// Execution levels computed from a workflow's DAG.
#[derive(Debug)]
pub struct ExecutionLevels {
    /// Nodes grouped by execution level (level 0 executes first)
    pub levels: Vec<Vec<String>>,
    /// Depth of each node (level index)
    #[allow(dead_code)]
    node_depths: HashMap<String, usize>,
}

impl ExecutionLevels {
    /// Compute execution levels from a workflow.
    ///
    /// Returns an error if the graph contains cycles.
    pub fn compute(workflow: &ExecutableWorkflow) -> Result<Self, String> {
        // Build adjacency lists
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

        for (node_id, node) in &workflow.nodes {
            in_degree.entry(node_id).or_insert(0);
            dependents.entry(node_id).or_default();

            for dep_id in &node.depends_on {
                *in_degree.entry(node_id).or_insert(0) += 1;
                dependents.entry(dep_id.as_str()).or_default().push(node_id);
            }
        }

        // Kahn's algorithm for topological sort with level tracking
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut node_depths: HashMap<String, usize> = HashMap::new();
        let mut processed = 0;

        // Entry nodes are at level 0
        for &node_id in &queue {
            node_depths.insert(node_id.to_string(), 0);
        }

        while let Some(node_id) = queue.pop_front() {
            processed += 1;
            let current_depth = node_depths[node_id];

            if let Some(deps) = dependents.get(node_id) {
                for &dep_id in deps {
                    if let Some(deg) = in_degree.get_mut(dep_id) {
                        *deg -= 1;

                        // Update depth to be max of all dependencies + 1
                        let new_depth = current_depth + 1;
                        let entry = node_depths.entry(dep_id.to_string()).or_insert(0);
                        *entry = (*entry).max(new_depth);

                        if *deg == 0 {
                            queue.push_back(dep_id);
                        }
                    }
                }
            }
        }

        // Check for cycles
        if processed != workflow.nodes.len() {
            return Err("Circular dependency detected".to_string());
        }

        // Group nodes by level
        let max_depth = node_depths.values().copied().max().unwrap_or(0);
        let mut levels: Vec<Vec<String>> = vec![Vec::new(); max_depth + 1];

        for (node_id, depth) in &node_depths {
            levels[*depth].push(node_id.clone());
        }

        // Sort nodes within each level for deterministic ordering
        for level in &mut levels {
            level.sort();
        }

        Ok(Self {
            levels,
            node_depths,
        })
    }

    /// Get the total number of levels.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.levels.len()
    }

    /// Get the depth of a node.
    #[cfg(test)]
    pub fn depth(&self, node_id: &str) -> Option<usize> {
        self.node_depths.get(node_id).copied()
    }

    /// Get nodes at a specific level.
    #[cfg(test)]
    pub fn nodes_at_level(&self, level: usize) -> &[String] {
        self.levels.get(level).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Iterate over levels.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &[String])> {
        self.levels
            .iter()
            .enumerate()
            .map(|(i, v)| (i, v.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::types::ExecutableNode;
    use std::collections::HashMap;

    fn make_workflow(nodes: Vec<(&str, Vec<&str>)>) -> ExecutableWorkflow {
        let mut node_map = HashMap::new();
        for (id, deps) in nodes {
            node_map.insert(
                id.to_string(),
                ExecutableNode {
                    id: id.to_string(),
                    node_type: "test".to_string(),
                    depends_on: deps.into_iter().map(|s| s.to_string()).collect(),
                    inputs: HashMap::new(),
                    condition: None,
                    timeout_seconds: None,
                    max_retries: None,
                },
            );
        }

        ExecutableWorkflow {
            name: "test".to_string(),
            description: None,
            version: 1,
            input_schema: HashMap::new(),
            nodes: node_map,
            outputs: HashMap::new(),
        }
    }

    #[test]
    fn test_linear_chain() {
        // a -> b -> c
        let workflow = make_workflow(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);

        let levels = ExecutionLevels::compute(&workflow).unwrap();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels.nodes_at_level(0), &["a"]);
        assert_eq!(levels.nodes_at_level(1), &["b"]);
        assert_eq!(levels.nodes_at_level(2), &["c"]);
    }

    #[test]
    fn test_parallel_nodes() {
        // a, b both entry -> c depends on both
        let workflow = make_workflow(vec![("a", vec![]), ("b", vec![]), ("c", vec!["a", "b"])]);

        let levels = ExecutionLevels::compute(&workflow).unwrap();

        assert_eq!(levels.len(), 2);
        assert!(levels.nodes_at_level(0).contains(&"a".to_string()));
        assert!(levels.nodes_at_level(0).contains(&"b".to_string()));
        assert_eq!(levels.nodes_at_level(1), &["c"]);
    }

    #[test]
    fn test_diamond_pattern() {
        //     a
        //    / \
        //   b   c
        //    \ /
        //     d
        let workflow = make_workflow(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);

        let levels = ExecutionLevels::compute(&workflow).unwrap();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels.nodes_at_level(0), &["a"]);
        assert!(levels.nodes_at_level(1).contains(&"b".to_string()));
        assert!(levels.nodes_at_level(1).contains(&"c".to_string()));
        assert_eq!(levels.nodes_at_level(2), &["d"]);
    }

    #[test]
    fn test_complex_dag() {
        //   a     b
        //   |    /|
        //   c   d |
        //    \ /  |
        //     e   |
        //      \ /
        //       f
        let workflow = make_workflow(vec![
            ("a", vec![]),
            ("b", vec![]),
            ("c", vec!["a"]),
            ("d", vec!["b"]),
            ("e", vec!["c", "d"]),
            ("f", vec!["e", "b"]),
        ]);

        let levels = ExecutionLevels::compute(&workflow).unwrap();

        assert!(levels.nodes_at_level(0).contains(&"a".to_string()));
        assert!(levels.nodes_at_level(0).contains(&"b".to_string()));
        assert_eq!(levels.depth("f"), Some(3));
    }

    #[test]
    fn test_cycle_detection() {
        // a -> b -> c -> a (cycle)
        let workflow = make_workflow(vec![("a", vec!["c"]), ("b", vec!["a"]), ("c", vec!["b"])]);

        let result = ExecutionLevels::compute(&workflow);
        assert!(result.is_err());
    }
}
