use palm::palm::node::*;
use palm::palm::nodeptr::*;
use palm::palm::notthreadsafe::NotThreadSafe;
use palm::palm::query::*;
use palm::palm::tree::*;
use palm::palm::util::*;
use palm::palm::worker::*;

use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::sync::Arc;

type KeyType = u32;
const BATCH_SIZE: usize = 8192;
const NUM_BATCHES: usize = 512;
const NUM_THREADS: usize = 8;
const KEY_RANGE: KeyType = 10000;

fn validate<K, V>(node: &mut Node<K, V>, l: &K, r: &K)
where
    K: Ord + Clone + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
{
    if node.is_leaf() {
        for i in 0..node.keys.len() {
            assert!(
                l <= &node.keys[i] && &node.keys[i] < r,
                "{:?} <= {:?}[{}] < {:?}",
                l,
                node.keys[i],
                i,
                r
            );
        }
    } else {
        let mut lo = l.clone();
        for i in 0..node.keys.len() {
            assert!(
                l <= &node.keys[i] && &node.keys[i] < r,
                "{:?} <= {:?}[{}] < {:?}",
                l,
                node.keys[i],
                i,
                r
            );
            assert!(node.ptrs()[i].get_mut().parent == NodePtr::new(node as *mut _));
            validate(node.ptrs()[i].get_mut(), &lo, &node.keys[i]);
            lo = node.keys[i].clone();
        }
        let idx = node.keys.len();
        assert!(node.ptrs()[idx].get_mut().parent == NodePtr::new(node as *mut _));
        validate(node.ptrs()[idx].get_mut(), &lo, r);
    }
}

#[test]
fn test_palm() {
    // let seed = [1u8; 32];
    // let mut rng : StdRng = SeedableRng::from_seed(seed);
    let mut rng = thread_rng();

    let tree = Arc::new(NotThreadSafe::new(Palm::<KeyType, KeyType>::new(
        NUM_THREADS,
    )));
    let mut map = BTreeMap::new();
    for _ in 0..NUM_BATCHES {
        let mut ref_result = vec![];
        let mut batch = vec![];
        for j in 0..BATCH_SIZE {
            if j % 2 == 0 {
                let (k, v) = (rng.gen_range(0, KEY_RANGE), rng.gen_range(0, KEY_RANGE));
                let query = Query::Insertion {
                    k: k.clone(),
                    v: v.clone(),
                };
                batch.push(query.clone());
                ref_result.push((query, map.insert(k, v)));
            } else {
                let k = rng.gen_range(1, KEY_RANGE);
                let query = Query::Retrieval { k: k.clone() };
                batch.push(query.clone());
                ref_result.push((query, map.get(&k).map(|v| v.clone())));
            }
        }

        ref_result.sort_by_key(|p| p.0.clone());
        let mut result = Palm::run_batch(&tree, &mut batch);
        result.sort_by_key(|p| p.0.clone());

        assert_eq!(ref_result.len(), result.len());
        for i in 0..ref_result.len() {
            assert_eq!(ref_result[i], result[i]);
        }
        validate(tree.get().root.get_mut(), &0, &KEY_RANGE);
    }
}

#[test]
fn test_pool() {
    // let seed = [1u8; 32];
    // let mut rng : StdRng = SeedableRng::from_seed(seed);
    let mut rng = thread_rng();

    let tree = Arc::new(NotThreadSafe::new(Palm::<KeyType, KeyType>::new(
        NUM_THREADS,
    )));
    let mut wrapper = PalmWrapper::new(tree.clone(), NUM_THREADS);
    let mut map = BTreeMap::new();
    for _ in 0..NUM_BATCHES {
        let mut ref_result = vec![];
        let mut batch = vec![];
        for j in 0..BATCH_SIZE {
            if j % 2 == 0 {
                let (k, v) = (rng.gen_range(0, KEY_RANGE), rng.gen_range(0, KEY_RANGE));
                let query = Query::Insertion {
                    k: k.clone(),
                    v: v.clone(),
                };
                batch.push(query.clone());
                ref_result.push((query, map.insert(k, v)));
            } else {
                let k = rng.gen_range(1, KEY_RANGE);
                let query = Query::Retrieval { k: k.clone() };
                batch.push(query.clone());
                ref_result.push((query, map.get(&k).map(|v| v.clone())));
            }
        }

        ref_result.sort_by_key(|p| p.0.clone());
        let mut result = wrapper.run_batch(&mut batch);
        result.sort_by_key(|p| p.0.clone());

        assert_eq!(ref_result.len(), result.len());
        for i in 0..ref_result.len() {
            assert_eq!(ref_result[i], result[i]);
        }
        validate(tree.get().root.get_mut(), &0, &KEY_RANGE);
    }
}
