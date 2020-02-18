use super::node::Node;
use super::util::*;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ptr;

#[derive(Debug)]
pub struct NodePtr<K, V>(*mut Node<K, V>);
impl<K, V> NodePtr<K, V> {
    pub fn new(ptr: *mut Node<K, V>) -> Self {
        Self(ptr)
    }

    #[must_use]
    pub fn is_null(self) -> bool {
        let Self(raw_ptr) = self;
        raw_ptr.is_null()
    }

    #[must_use]
    pub fn as_ptr(self) -> *mut Node<K, V> {
        self.0
    }

    pub fn manually_drop(&mut self) {
        if !self.is_null() {
            let raw_ptr = self.0;
            self.0 = ptr::null_mut();
            let node = unsafe { Box::from_raw(raw_ptr) };
            drop(node);
            assert_eq!(self.is_null(), true);
        }
    }
}

impl<K, V> Clone for NodePtr<K, V> {
    #[must_use]
    fn clone(&self) -> Self {
        Self::new(self.0)
    }
}
impl<K, V> Copy for NodePtr<K, V> {}
unsafe impl<K, V> Send for NodePtr<K, V> {}
unsafe impl<K, V> Sync for NodePtr<K, V> {}

impl<K, V> Eq for NodePtr<K, V> {}
impl<K, V> PartialEq for NodePtr<K, V> {
    #[must_use]
    fn eq(&self, other: &Self) -> bool {
        let Self(p1) = self;
        let Self(p2) = other;
        assert_eq!(
            ptr::eq(p1.get_mut(), p2.get_mut()),
            *p1 as usize == *p2 as usize
        );
        *p1 as usize == *p2 as usize
    }
}
impl<K, V> PartialOrd for NodePtr<K, V> {
    #[must_use]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<K, V> Ord for NodePtr<K, V> {
    #[must_use]
    fn cmp(&self, other: &Self) -> Ordering {
        let Self(p1) = self;
        let Self(p2) = other;
        (*p1 as usize).cmp(&(*p2 as usize))
    }
}

impl<K, V> Hash for NodePtr<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self(p) = self;
        (*p as u64).hash(state);
    }
}
