/// ported from bplustree baseline
use super::nodeptr::NodePtr;
use super::util::*;
use super::vector::MyVector;

use std::ptr;

pub type Vector<T> = MyVector<T>;
const B: usize = 19;
pub const MIN_LEN: usize = B;
pub const MAX_LEN: usize = 2 * B;

#[derive(Debug)]
#[repr(C)]
pub struct Node<K, V> {
    // 512 bytes (4 * 64 (cache line))
    pub level: usize,          // 8 bytes
    pub parent: NodePtr<K, V>, // 8 bytes

    pub keys: Vector<K>,      // 168 bytes ~ (2*19 + 1) * 4 + 8 + 4 (align)
    elements: Elements<K, V>, // 328 bytes ~ (2*19 + 1) * 8 + 8 + 8
}

#[derive(Debug)]
pub enum Elements<K, V> {
    Vals(Vector<V>),
    Ptrs(Vector<NodePtr<K, V>>),
}

use Elements::{Ptrs, Vals};

impl<K, V> Node<K, V> {
    #[must_use]
    pub fn leaf_with(keys: Vector<K>, vals: Vector<V>, parent: NodePtr<K, V>) -> Box<Self> {
        Box::new(Self {
            keys,
            elements: Vals(vals),
            parent,
            level: 1,
        })
    }

    #[must_use]
    pub fn leaf() -> Box<Self> {
        Self::leaf_with(Vector::new(), Vector::new(), NodePtr::new(ptr::null_mut()))
    }

    #[must_use]
    pub fn internal_with(
        keys: Vector<K>,
        vals: Vector<NodePtr<K, V>>,
        parent: NodePtr<K, V>,
        level: usize,
    ) -> Box<Self> {
        Box::new(Self {
            keys,
            elements: Ptrs(vals),
            parent,
            level,
        })
    }

    #[must_use]
    pub fn internal(level: usize) -> Box<Self> {
        Self::internal_with(
            Vector::new(),
            Vector::new(),
            NodePtr::new(ptr::null_mut()),
            level,
        )
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn vals(&self) -> &Vector<V> {
        match &self.elements {
            Vals(vals) => vals,
            Ptrs(_) => panic!("Tried accessing values on an internal node"),
        }
    }

    pub fn vals_mut(&mut self) -> &mut Vector<V> {
        match &mut self.elements {
            Vals(vals) => vals,
            Ptrs(_) => panic!("Tried accessing values on an internal node"),
        }
    }

    pub fn ptrs(&mut self) -> &mut Vector<NodePtr<K, V>> {
        match &mut self.elements {
            Vals(_) => panic!("Tried accessing values on an internal node"),
            Ptrs(ptrs) => ptrs,
        }
    }

    #[must_use]
    pub fn is_leaf(&self) -> bool {
        self.level == 1
    }

    pub fn stat() {
        println!("Node size: {}", std::mem::size_of::<Self>());
        println!("- level: {}", std::mem::size_of::<usize>());
        println!("- parent: {}", std::mem::size_of::<NodePtr<K, V>>());
        println!("- keys: {}", std::mem::size_of::<MyVector<K>>());
        println!("- elements: {}", std::mem::size_of::<Elements<K, V>>());
    }
}

impl<K: std::fmt::Debug + Ord + Clone, V: Clone> Node<K, V> {
    pub fn search(&self, key: &K) -> Option<V> {
        let idx = self.keys.linear_search(key);
        if idx < self.len() {
            Some(self.vals()[idx].clone())
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        assert_eq!(self.is_leaf(), true);
        let idx = self.keys.linear_search(&key);
        if self.has_exact_key_at(idx, &key) {
            Some(std::mem::replace(&mut self.vals_mut()[idx], val))
        } else {
            self.keys.push(key);
            self.vals_mut().push(val);
            None
        }
    }

    pub fn index_of(&self, key: &K) -> usize {
        self.keys.linear_search(key)
    }

    pub fn val_at(&self, idx: usize) -> Option<V> {
        if idx < self.len() {
            Some(self.vals()[idx].clone())
        } else {
            None
        }
    }

    pub fn has_exact_key_at(&self, idx: usize, key: &K) -> bool {
        idx < self.len() && self.keys[idx] == *key
    }

    pub fn find_leaf(&mut self, key: &K) -> &mut Self {
        if self.is_leaf() {
            self
        } else {
            let idx = self.keys.upper_bound(key);
            assert!(idx <= self.keys.len());
            assert!(idx < self.ptrs().len());
            self.ptrs()[idx].get_mut().find_leaf(key)
        }
    }
}

impl<K, V> Drop for Node<K, V> {
    fn drop(&mut self) {
        if !self.is_leaf() {
            for child in self.ptrs() {
                child.manually_drop();
            }
        }
    }
}
