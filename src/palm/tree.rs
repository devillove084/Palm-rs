use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::{Arc, Barrier};
use std::thread;

use super::modification::Modification as Modif;
use super::node::{Node, MAX_LEN, MIN_LEN};
use super::nodeptr::NodePtr;
use super::notthreadsafe::NotThreadSafe;
use super::query::Query;
use super::util::*;
use super::worker::Worker;

const Q: usize = 64;

pub type MapType<K, V> = HashMap<K, V>;
pub type WorkMap<K, V, T> = MapType<NodePtr<K, V>, Vec<T>>;
pub type ModifMap<K, V> = VecDeque<(NodePtr<K, V>, Vec<Modif<K, V>>)>;
pub type QueryMap<K, V> = VecDeque<(NodePtr<K, V>, Vec<Query<K, V>>)>;
enum Elements<K, V> {
    Vals(Vec<V>),
    Ptrs(Vec<NodePtr<K, V>>),
}

#[allow(non_snake_case)]
pub struct Palm<K, V>
where
    K: Clone,
    V: Clone,
{
    pub depth: usize,
    pub root: NodePtr<K, V>,
    pub num_threads: usize,
}

unsafe impl<K: Clone, V: Clone> Sync for Palm<K, V> {}
unsafe impl<K: Clone, V: Clone> Send for Palm<K, V> {}

impl<K, V> Palm<K, V>
where
    K: 'static + Ord + Clone + std::fmt::Debug,
    V: 'static + Clone + std::fmt::Debug,
{
    #[must_use]
    #[allow(non_snake_case)]
    pub fn new(num_threads: usize) -> Self {
        Node::<K, V>::stat();
        Self {
            depth: 1,
            root: NodePtr::new(Box::into_raw(Node::<K, V>::leaf())),
            num_threads,
        }
    }

    pub fn partition<T: Clone>(batch: &[T], t: usize) -> Vec<Vec<T>> {
        batch
            .chunks((batch.len() + t - 1) / t)
            .map(|x| x.to_vec())
            .collect()
    }

    pub fn search(
        queries: &mut Vec<Query<K, V>>,
        curr_query: &NotThreadSafe<QueryMap<K, V>>,
        root: NodePtr<K, V>,
    ) {
        // Latency Hiding:
        //   1. Use BFS instead of DFS for better locality
        //   2. Prefetch child node to visit
        // Ideally, Q is chosen such that the computation time
        //   to find the next child node for all Q is at least
        //   as much as the latency of fetching a node from main
        //   memory.
        // (Q is the width of BFS)
        let mut paths = vec![root; queries.len()];
        let mut base = 0;
        for chunk in queries.chunks(Q) {
            for level in (1..root.get_mut().level).rev() {
                for (i, query) in chunk.iter().enumerate() {
                    unsafe {
                        // prefetch sibling
                        if base + i + 1 < paths.len() {
                            std::intrinsics::prefetch_read_data(
                                paths[base + i + 1].as_ptr() as *const _,
                                2,
                            );
                        }
                    }
                    let node = paths[base + i].get_mut();
                    let idx = node.keys.upper_bound(&query.get_key());
                    paths[base + i] = node.ptrs()[idx];
                    unsafe {
                        if level != 1 {
                            // prefetch child
                            std::intrinsics::prefetch_read_data(
                                paths[base + i].as_ptr() as *const _,
                                2,
                            );
                        }
                    }
                }
            }
            base += Q;
        }

        // Reuse previous deque
        let query_guard = curr_query.get_mut();
        let mut idx = 0;
        for (i, query) in queries.drain(..).enumerate() {
            if idx > 0 && query_guard[idx - 1].0 == paths[i] {
                query_guard[idx - 1].1.push(query);
            } else {
                if idx < query_guard.len() {
                    query_guard[idx].0 = paths[i];
                    query_guard[idx].1.push(query);
                } else {
                    query_guard.push_back((paths[i], vec![query]));
                }
                idx += 1;
            }
        }
        query_guard.truncate(idx);
    }

    pub fn redistribute_work<T: std::fmt::Debug + Clone>(
        thread_index: usize,
        input: &[NotThreadSafe<VecDeque<(NodePtr<K, V>, Vec<T>)>>],
        _num_threads: usize,
        their_last: &mut NodePtr<K, V>,
    ) {
        // initialize
        let curr_layer = input[thread_index].get_mut();
        if curr_layer.is_empty() {
            return;
        }

        // remove work that assigned to previous threads
        if their_last.is_null() {
            for map in input.iter().take(thread_index).rev() {
                if let Some((node, _)) = map.get().back() {
                    if *node == curr_layer[0].0 {
                        std::mem::replace(their_last, *node);
                    }
                    break;
                }
            }
        }

        if curr_layer.back().unwrap().0 == *their_last {
            return;
        }

        if curr_layer.back().unwrap().0 != *their_last {
            // steal work from following threads
            //   Note here we cannot use "their_first" to accelerate because
            //     "their_first" is not necessarily the first element of immediate
            //     next thread
            //   (recall that if there is only one element in the queue, a thread
            //     would use next thread's first element as its "their_first")
            for map in input.iter().skip(thread_index + 1) {
                if let Some((node, modif)) = map.get_mut().front_mut() {
                    if *node == curr_layer.back().unwrap().0 {
                        curr_layer.back_mut().unwrap().1.extend(modif.drain(..));
                    } else {
                        break;
                    }
                }
            }
        }
    }

    #[allow(non_snake_case)]
    fn big_split(
        node: &mut Node<K, V>,
        keys: &mut Vec<K>,
        vals: &mut Elements<K, V>,
    ) -> Vec<(K, NodePtr<K, V>)> {
        let mut splits = Vec::new();
        while keys.len() > MAX_LEN {
            let len = keys.len();
            match vals {
                Elements::Ptrs(ptrs) => {
                    let new_node = NodePtr::new(Box::into_raw(Node::internal_with(
                        keys.split_off(len - MIN_LEN).into(),
                        ptrs.split_off(len - MIN_LEN).into(),
                        node.parent,
                        node.level,
                    )));
                    for child in new_node.get_mut().ptrs() {
                        child.get_mut().parent = new_node;
                    }
                    let new_key = keys.pop().unwrap();
                    splits.push((new_key, new_node));
                }
                Elements::Vals(vals) => {
                    let new_node = NodePtr::new(Box::into_raw(Node::leaf_with(
                        keys.split_off(len - MIN_LEN).into(),
                        vals.split_off(len - MIN_LEN).into(),
                        node.parent,
                    )));
                    let new_key = new_node.get().keys[0].clone();
                    splits.push((new_key, new_node));
                }
            }
        }
        splits.reverse();
        splits
    }

    #[allow(non_snake_case)]
    fn maybe_split(
        node: &mut Node<K, V>,
        keys: &mut Vec<K>,
        vals: &mut Elements<K, V>,
    ) -> Option<Vec<(K, NodePtr<K, V>)>> {
        let ret = if keys.len() > MAX_LEN {
            Some(Self::big_split(node, keys, vals))
        } else {
            None
        };

        node.keys.clone_from_vec(keys);
        keys.clear();
        match vals {
            Elements::Vals(vals) => {
                node.vals_mut().clone_from_vec(vals);
                vals.clear();
            }
            Elements::Ptrs(ptrs) => {
                node.ptrs().clone_from_vec(ptrs);
                ptrs.clear();
            }
        }
        ret
    }

    fn try_lookup(keys: &[K], vals: &[V], key: &K) -> Option<V> {
        // As keys of queries is non-decreasing, we only need to
        //   compare with the last element
        if Some(key) == keys.last() {
            vals.last().map(|v| v.clone())
        } else {
            None
        }
    }

    fn try_insert(keys: &mut Vec<K>, vals: &mut Vec<V>, key: K, val: V) -> Option<V> {
        if Some(&key) == keys.last() {
            Some(std::mem::replace(vals.last_mut().unwrap(), val))
        } else {
            keys.push(key);
            vals.push(val);
            None
        }
    }

    #[allow(non_snake_case)]
    pub fn apply_to_leaf_nodes(
        curr_query: &NotThreadSafe<QueryMap<K, V>>,
        next_modif: &NotThreadSafe<ModifMap<K, V>>,
        their_last: NodePtr<K, V>,
    ) -> Vec<(Query<K, V>, Option<V>)> {
        let mut results: Vec<(Query<K, V>, Option<V>)> = Vec::new();
        let curr_map = curr_query.get_mut();
        let next_map = next_modif.get_mut();
        next_map.clear();

        // buffer
        let mut keys: Vec<K> = Vec::new();
        let mut vals: Vec<V> = Vec::new();
        for (i, (node_ptr, queries)) in curr_map.iter_mut().enumerate() {
            unsafe {
                if i + 1 < curr_query.get_mut().len() {
                    std::intrinsics::prefetch_write_data(curr_query.get_mut()[i + 1].0.as_ptr(), 2);
                }
            }

            let node = node_ptr.get_mut();
            if !their_last.is_null() && *node_ptr == their_last {
                continue;
            }

            keys.clear();
            vals.clear();

            for query in queries.drain(..) {
                let idx = node.index_of(query.get_key());
                let result = match &query {
                    Query::Retrieval { k } => node
                        .val_at(idx)
                        .or_else(|| Self::try_lookup(&keys, &vals, k)),
                    Query::Insertion { k, v } => node
                        .val_at(idx)
                        .and_then(|_| Some(std::mem::replace(&mut node.vals_mut()[idx], v.clone())))
                        .or_else(|| {
                            Self::try_insert(&mut keys, &mut vals, k.clone(), v.clone()).or(None)
                        }),
                };
                results.push((query, result));
            }

            if node.len() + keys.len() <= MAX_LEN {
                for (k, v) in keys.drain(..).zip(vals.drain(..)) {
                    node.keys.push(k);
                    node.vals_mut().push(v);
                }
            } else {
                keys.extend(node.keys.iter().map(|k| k.clone()));
                vals.extend(node.vals().iter().map(|v| v.clone()));
                let mut indices: Vec<_> = (0..keys.len()).collect();
                indices.sort_by_key(|i| keys[*i].clone());
                let mut new_keys = Vec::with_capacity(keys.len());
                let mut new_vals = Vec::with_capacity(vals.len());
                for idx in indices {
                    new_keys.push(keys[idx].clone());
                    new_vals.push(vals[idx].clone());
                }
                std::mem::swap(&mut new_keys, &mut keys);
                std::mem::swap(&mut new_vals, &mut vals);

                let mut temp_vals = Elements::Vals(vals);
                if let Some(nodes) = Self::maybe_split(node, &mut keys, &mut temp_vals) {
                    let modif = Modif::Overflow {
                        nodes,
                        orphan: Vec::new(),
                    };
                    if !next_map.is_empty() && next_map.back().unwrap().0 == node.parent {
                        next_map.back_mut().unwrap().1.push(modif);
                    } else {
                        next_map.push_back((node.parent, vec![modif]));
                    }
                }
                vals = match temp_vals {
                    Elements::Vals(temp) => temp,
                    _ => panic!("Should never be here. "),
                }
            }
        }
        results
    }

    pub fn apply_to_internal_nodes(
        curr_modif: &NotThreadSafe<ModifMap<K, V>>,
        next_modif: &NotThreadSafe<ModifMap<K, V>>,
        their_last: NodePtr<K, V>,
    ) {
        let next_map = next_modif.get_mut();
        next_map.clear();

        let mut keys: Vec<_> = Vec::new();
        let mut ptrs: Vec<_> = Vec::new();
        for (node_ptr, modifs) in curr_modif.get_mut() {
            assert_eq!(node_ptr.is_null(), false);
            if !their_last.is_null() && *node_ptr == their_last {
                continue;
            }

            let node = node_ptr.get_mut();
            keys.clear();
            keys.extend(node.keys.clone().to_vec());
            ptrs.clear();
            ptrs.extend(node.ptrs().clone().to_vec());

            for modif in modifs {
                match modif {
                    Modif::Overflow { nodes, .. } => {
                        for (k, child) in nodes.iter() {
                            let idx = keys.lower_bound(&k);
                            keys.insert(idx, k.clone());
                            ptrs.insert(idx + 1, *child);
                        }
                    }
                    _ => {}
                }
            }

            let mut temp_ptrs = Elements::Ptrs(ptrs);
            if let Some(nodes) = Self::maybe_split(node, &mut keys, &mut temp_ptrs) {
                let modif = Modif::Overflow {
                    nodes,
                    orphan: Vec::new(),
                };
                if !next_map.is_empty() && next_map.back().unwrap().0 == node.parent {
                    next_map.back_mut().unwrap().1.push(modif);
                } else {
                    next_map.push_back((node.parent, vec![modif]));
                }
            }
            ptrs = match temp_ptrs {
                Elements::Ptrs(temp) => temp,
                _ => panic!("Should never be here. "),
            }
        }
    }

    #[allow(non_snake_case)]
    pub fn handle_root(
        tree_ptr: &Arc<NotThreadSafe<Self>>,
        modifs_list: &[NotThreadSafe<ModifMap<K, V>>],
    ) {
        // collect all the modifs
        let mut collected = Vec::new();
        for modifs in modifs_list {
            for (node_ptr, mut modifs) in modifs.get_mut().drain(..) {
                assert_eq!(node_ptr.is_null(), true);
                collected.extend(modifs.drain(..));
            }
        }

        while !collected.is_empty() {
            // create new root
            let tree = tree_ptr.get_mut();
            let old_root = tree.root;
            tree.root = NodePtr::new(Box::into_raw(Node::internal(old_root.get_mut().level + 1)));
            old_root.get_mut().parent = tree.root;
            tree.root.get_mut().ptrs().push(old_root);
            tree.depth += 1;

            assert!(!tree.root.get_mut().ptrs().is_empty());

            let node = tree.root.get_mut();
            let mut keys: Vec<_> = mem::replace(&mut node.keys, unsafe {
                mem::MaybeUninit::uninit().assume_init()
            })
            .into();
            let mut ptrs: Vec<_> = mem::replace(node.ptrs(), unsafe {
                mem::MaybeUninit::uninit().assume_init()
            })
            .into();

            let mut work = Vec::new();
            std::mem::swap(&mut collected, &mut work);
            for modif in work.drain(..) {
                match modif {
                    Modif::Overflow { mut nodes, .. } => {
                        for (k, child) in nodes.drain(..) {
                            assert_eq!(child.get_mut().parent.is_null(), true);
                            let idx = keys.lower_bound(&k);
                            keys.insert(idx, k);
                            ptrs.insert(idx + 1, child);
                            child.get_mut().parent = tree.root;
                        }
                    }
                    _ => {}
                }
            }

            if let Some(nodes) = Self::maybe_split(node, &mut keys, &mut Elements::Ptrs(ptrs)) {
                collected.push(Modif::Overflow {
                    nodes,
                    orphan: Vec::new(),
                });
            }
        }
    }

    pub fn run_batch(
        tree: &Arc<NotThreadSafe<Self>>,
        queries: &mut Vec<Query<K, V>>,
    ) -> Vec<(Query<K, V>, Option<V>)>
    where
        K: Ord + std::fmt::Debug + Clone + Send + Sync + 'static,
        V: Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        // by sorting in advance, redistribution can be significantly simplied
        //   note that sort has to be stable to preserve the order of queries
        queries.sort();

        let num_threads = tree.get().num_threads;
        let mut chunks = Self::partition(&queries, num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));
        let q_query: Arc<Vec<_>> = Arc::new(
            (0..2)
                .map(|_| {
                    (0..num_threads)
                        .map(|_| NotThreadSafe::new(VecDeque::new()))
                        .collect()
                })
                .collect(),
        );
        let q_modif: Arc<Vec<_>> = Arc::new(
            (0..2)
                .map(|_| {
                    (0..num_threads)
                        .map(|_| NotThreadSafe::new(VecDeque::new()))
                        .collect()
                })
                .collect(),
        );
        let first: Arc<NotThreadSafe<Vec<_>>> = Arc::new(NotThreadSafe::new(
            (0..num_threads).map(|_| Vec::new()).collect(),
        ));
        let last: Arc<NotThreadSafe<Vec<_>>> = Arc::new(NotThreadSafe::new(
            (0..num_threads).map(|_| Vec::new()).collect(),
        ));
        let mut handles = vec![];
        for (thread_idx, chunk) in chunks.drain(..).enumerate() {
            let p_barrier = barrier.clone();
            let p_tree = tree.clone();
            let p_q = q_query.clone();
            let p_m = q_modif.clone();
            let p_f = first.clone();
            let p_l = last.clone();
            handles.push(thread::spawn(move || {
                let worker = Worker::new(thread_idx, p_tree.clone(), p_barrier, p_m, p_q, p_f, p_l);
                worker.execute(chunk)
            }));
        }

        let mut result = vec![];
        for handle in handles {
            result.extend(handle.join().unwrap());
        }
        result
    }
}

impl<K: Clone, V: Clone> Drop for Palm<K, V> {
    fn drop(&mut self) {
        self.root.manually_drop();
    }
}
