// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::fmt;

use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, LogicalFilter, LogicalJoin, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary,
    ToBatch, ToStream,
};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalMultiJoin` combines two or more relations according to some condition.
///
/// Each output row has fields from one the inputs. The set of output rows is a subset
/// of the cartesian product of all the inputs; The `LogicalMultiInnerJoin` is only supported
/// for inner joins as it implicitly assumes commutativity. Non-inner joins should be
/// expressed as 2-way `LogicalJoin`s.
#[derive(Debug, Clone)]
pub struct LogicalMultiJoin {
    pub base: PlanBase,
    inputs: Vec<PlanRef>,
    on: Condition,
}

impl fmt::Display for LogicalMultiJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalMultiJoin {{ on: {} }}", &self.on)
    }
}

impl LogicalMultiJoin {
    pub(crate) fn new(base: PlanBase, inputs: Vec<PlanRef>, on: Condition) -> Self {
        Self { base, inputs, on }
    }

    pub(crate) fn from_join(join: &PlanRef) -> Option<Self> {
        let logical_join = join.as_logical_join()?;
        if logical_join.join_type() != JoinType::Inner {
            return None;
        }
        let left = logical_join.left();
        let right = logical_join.right();

        let left_col_num = left.schema().len();
        let right_col_num = right.schema().len();

        let mut inputs = vec![];
        let mut conjunctions = logical_join.on().conjunctions.clone();
        if let Some(multi_join) = left.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            conjunctions.extend(multi_join.on().clone());
        } else {
            inputs.push(left.clone());
        }
        if let Some(multi_join) = right.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            let right_on = multi_join.on().clone();
            let mut mapping =
                ColIndexMapping::with_shift_offset(right_col_num, left_col_num as isize);
            let new_on = right_on.rewrite_expr(&mut mapping);
            conjunctions.extend(new_on.conjunctions);
        } else {
            inputs.push(right.clone());
        }

        Some(Self {
            base: logical_join.base.clone(),
            inputs,
            on: Condition { conjunctions },
        })
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.base.clone(), self.inputs.clone(), cond)
    }
}

impl PlanTreeNode for LogicalMultiJoin {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(
        &self,
        _inputs: &[crate::optimizer::PlanRef],
    ) -> crate::optimizer::PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

#[derive(Debug)]
struct ConnectedComponentLabeller {
    vertex_to_label: HashMap<usize, usize>,
    labels_to_vertices: HashMap<usize, HashSet<usize>>,
    labels_to_edges: HashMap<usize, HashSet<(usize, usize)>>,
}

impl ConnectedComponentLabeller {
    fn new(vertices: usize) -> Self {
        let mut vertex_to_label = HashMap::with_capacity(vertices);
        let mut labels_to_vertices = HashMap::with_capacity(vertices);
        let labels_to_edges = HashMap::new();
        for i in 0..vertices {
            vertex_to_label.insert(i, i);
            labels_to_vertices.insert(i, vec![i].into_iter().collect());
        }
        Self {
            vertex_to_label,
            labels_to_vertices,
            labels_to_edges,
        }
    }

    fn add_edge(&mut self, v1: usize, v2: usize) {
        let v1_label = *self.vertex_to_label.get(&v1).unwrap();
        let v2_label = *self.vertex_to_label.get(&v2).unwrap();

        let (new_label, old_label) = if v1_label < v2_label {
            (v1_label, v2_label)
        } else {
            // v1_label > v2_label
            (v2_label, v1_label)
        };

        {
            let edges = self
                .labels_to_edges
                .entry(new_label)
                .or_insert_with(HashSet::new);

            let new_edge = if v1 < v2 { (v1, v2) } else { (v2, v1) };
            edges.insert(new_edge);
        }

        if v1_label == v2_label {
            return;
        }

        // Reassign to the smaller label
        let old_vertices = self.labels_to_vertices.remove(&old_label).unwrap();
        self.labels_to_vertices
            .get_mut(&new_label)
            .unwrap()
            .extend(old_vertices.iter());
        for v in old_vertices {
            self.vertex_to_label.insert(v, new_label);
        }
        if let Some(old_edges) = self.labels_to_edges.remove(&old_label) {
            let edges = self
                .labels_to_edges
                .entry(new_label)
                .or_insert_with(HashSet::new);
            edges.extend(old_edges);
        }
    }

    fn get_labels(&self) -> Vec<HashSet<(usize, usize)>> {
        self.labels_to_edges.into_values().collect()
    }
}

// Graph: 0-1-2  3-4-5  6
// => 0-1-2-3-4-5  6
#[test]
fn test_get_connected_components() {
    let mut labeller = ConnectedComponentLabeller::new(7);
    labeller.add_edge(0, 1);
    labeller.add_edge(1, 2);

    labeller.add_edge(3, 4);
    labeller.add_edge(4, 5);

    assert_eq!(labeller.labels_to_vertices.len(), 3);

    labeller.add_edge(2, 3);

    assert_eq!(labeller.labels_to_vertices.len(), 2);

    labeller.add_edge(5, 6);

    assert_eq!(labeller.labels_to_vertices.len(), 1);
    assert_eq!(
        *labeller.labels_to_vertices.iter().next().unwrap().1,
        (0..7).collect::<HashSet<_>>()
    );
    assert_eq!(
        *labeller.labels_to_edges.iter().next().unwrap().1,
        vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]
            .into_iter()
            .collect::<HashSet<_>>()
    );
}

impl LogicalMultiJoin {
    pub fn as_left_deep_join_with_ordering(&self, ordering: &[usize]) -> Result<PlanRef> {
        assert_eq!(ordering.len(), self.input.len());
        
    }

    // Our heuristic join reordering algorithm will try to perform a left-deep join.
    // It will try to do the following:
    //
    // 1. First, split the join graph, with eq join conditions as graph edges, into their connected
    //    components. Repeat the procedure in 2. with the largest connected components down to
    //    the smallest.
    // 2. For each connected component, add joins to the chain, prioritizing adding those
    //    joins to the bottom of the chain if their join conditions have:
    //       a. eq joins between primary keys on both sides
    //       b. eq joins with primary keys on one side
    //       c. more equijoin conditions
    //    in that order. This forms our selectivity heuristic.
    // 3. Thirdly, we will emit a left-deep cross-join of each of the left-deep joins of the
    //    connected components. Depending on the type of plan, this may result in a planner failure
    //    (e.g. for streaming). No cross-join will be emitted for a single connected component.
    // 4. Finally, we will emit, above the left-deep join tree:
    //        a. a filter with the non eq conditions
    //        b. a projection which reorders the output column ordering to agree with the
    //           original ordering of the joins.
    //    The filter will then be pushed down by another filter pushdown pass.
    pub(crate) fn to_left_deep_join_with_heuristic_ordering(&self) -> Result<PlanRef> {
        let mut labels = ConnectedComponentLabeller::new(self.inputs.len());

        let (mut join_conditions, non_eq_cond) = self
            .on
            .clone()
            .split_eq_by_input_col_nums(&self.input_col_nums());

        // Iterate over all join conditions, whose keys represent edges on the join graph
        for k in join_conditions.keys() {
            labels.add_edge(k.0, k.1);
        }

        let mut connected_components: Vec<_> = labels.get_labels();

        // Sort in decreasing order of len
        connected_components.sort_by_key(|a| std::cmp::Reverse(a.len()));

        let mut left_deep_joins = Vec::<PlanRef>::with_capacity(connected_components.len());
        let mut join_ordering = vec![];

        for component in connected_components {
            let mut conditions = vec![];
            for edge in component {
                // Technically, every edge should be in join condition
                if let Some(condition) = join_conditions.remove(&edge) {
                    conditions.push((edge, condition));
                }
            }

            let (mut join, join_ordering_start_index) = if !conditions.is_empty() {
                let (edge, condition) = conditions.remove(0);
                let join_ordering_start_index = join_ordering.len();
                join_ordering.append(&mut vec![edge.0, edge.1]);

                let mut mapping = self.mapping_from_ordering(&join_ordering).inverse();
                let remapped_condition = condition.rewrite_expr(&mut mapping);

                (
                    LogicalJoin::new(
                        self.inputs[edge.0].clone(),
                        self.inputs[edge.1].clone(),
                        JoinType::Inner,
                        remapped_condition,
                    ),
                    join_ordering_start_index,
                )
            } else {
                // There is nothing to join in this connected component
                break;
            };

            while !conditions.is_empty() {
                let mut found = vec![];
                for (idx, (edge, condition)) in conditions.iter().enumerate() {
                    // If the eq join condition is on the existing join, add it to the existing
                    // join's on condition (this will be pushed down further later on).
                    if join_ordering.contains(&edge.1) && join_ordering.contains(&edge.0) {
                        let mut remapped_condition = condition.clone();
                        let mut mapping = self
                            .mapping_from_ordering(&join_ordering[join_ordering_start_index..])
                            .inverse();
                        remapped_condition = remapped_condition.rewrite_expr(&mut mapping);

                        let new_on = join.on().clone().and(remapped_condition);

                        join = join.clone_with_cond(new_on);
                        found.push(idx);
                    } else {
                        // Else, the eq join condition involves a new input, or is not connected to
                        // the existing left deep tree. Handle accordingly.
                        let new_input = if join_ordering.contains(&edge.0) {
                            edge.1
                        } else if join_ordering.contains(&edge.1) {
                            edge.0
                        } else {
                            continue;
                        };
                        join_ordering.push(new_input);
                        found.push(idx);

                        let mut remapped_condition = condition.clone();
                        let mut mapping = self
                            .mapping_from_ordering(&join_ordering[join_ordering_start_index..])
                            .inverse();
                        remapped_condition = remapped_condition.rewrite_expr(&mut mapping);

                        join = LogicalJoin::new(
                            join.into(),
                            self.inputs[new_input].clone(),
                            JoinType::Inner,
                            remapped_condition,
                        );
                    }
                }
                // This ensures conditions.len() is strictly decreasing per iteration
                // Since the graph is connected, it is always possible to find at least one edge
                // remaining that can be connected to the current join result.
                if found.is_empty() {
                    return Err(RwError::from(ErrorCode::InternalError(
                        "Connecting edge not found in join connected subgraph".into(),
                    )));
                }
                let mut idx = 0;
                conditions.retain(|_| {
                    let keep = !found.contains(&idx);
                    idx += 1;
                    keep
                });
            }
            left_deep_joins.push(join.into());
        }
        // Deal with singleton inputs (with no eq condition joins between them whatsoever)
        for i in 0..self.inputs.len() {
            if !join_ordering.contains(&i) {
                join_ordering.push(i);
                left_deep_joins.push(self.inputs[i].clone());
            }
        }
        assert_eq!(
            join_conditions.len(),
            0,
            "REMAINING JOIN CONDITIONS {:?}",
            join_conditions
        );
        let mut left_deep_joins_iter = left_deep_joins.iter();
        let base_join =
            left_deep_joins_iter
                .next()
                .ok_or(RwError::from(ErrorCode::InternalError(
                    "No relations found in the MultiJoin".into(),
                )))?;

        // Create a bushy join by cross-joining a series of joins with no join graph edge
        // between them in a left-deep fashion.
        let mut output: PlanRef =
            left_deep_joins_iter.fold(base_join.clone(), |join: PlanRef, next_join| {
                LogicalJoin::new(
                    join,
                    next_join.clone().into(),
                    JoinType::Inner,
                    Condition {
                        conjunctions: vec![],
                    },
                )
                .into()
            });

        if join_ordering != (0..self.input_col_nums().iter().sum()).collect::<Vec<_>>() {
            output =
                LogicalProject::with_mapping(output, self.mapping_from_ordering(&join_ordering));
        }

        // We will later push down the `non_eq_cond` back to the individual joins via the
        // `FilterJoinRule`.
        output = LogicalFilter::create(output, non_eq_cond);

        Ok(output)
    }

    pub(crate) fn input_col_nums(&self) -> Vec<usize> {
        self.inputs.iter().map(|i| i.schema().len()).collect()
    }

    pub(crate) fn mapping_from_ordering(&self, ordering: &[usize]) -> ColIndexMapping {
        let offsets = self.input_col_offsets();
        let max_len = offsets[self.inputs.len()];
        let mut map = Vec::with_capacity(self.schema().len());
        let input_num_cols = self.input_col_nums();
        for &input_index in ordering {
            map.extend(
                (offsets[input_index]..offsets[input_index] + input_num_cols[input_index])
                    .map(Some),
            )
        }
        ColIndexMapping::with_target_size(map, max_len)
    }

    fn input_col_offsets(&self) -> Vec<usize> {
        self.inputs().iter().fold(vec![0], |mut v, i| {
            v.push(v.last().unwrap() + i.schema().len());
            v
        })
    }
}

impl ToStream for LogicalMultiJoin {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }

    fn to_stream(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ToBatch for LogicalMultiJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ColPrunable for LogicalMultiJoin {
    fn prune_col(&self, _required_cols: &[usize]) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}
