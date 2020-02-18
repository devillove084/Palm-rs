use super::nodeptr::NodePtr;

#[derive(Debug, Clone)]
pub enum Modification<K: Ord + Clone, V: Clone> {
    Overflow {
        nodes: Vec<(K, NodePtr<K, V>)>,
        orphan: Vec<(K, V)>,
    },
    Underflow {
        nodes: Vec<(K, NodePtr<K, V>)>,
        orphan: Vec<(K, V)>,
    },
}
