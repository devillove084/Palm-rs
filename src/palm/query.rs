use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub enum Query<K, V> {
    Retrieval { k: K },
    Insertion { k: K, v: V },
}

impl<K: Ord + Clone, V: Clone> Query<K, V> {
    pub fn get_key(&self) -> &K {
        match self {
            Self::Retrieval { k } | Self::Insertion { k, .. } => k,
        }
    }
}

impl<K: Ord + Clone, V: Clone> Ord for Query<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_key().cmp(&other.get_key())
    }
}
impl<K: Ord + Clone, V: Clone> PartialOrd for Query<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<K: Ord + Clone, V: Clone> PartialEq for Query<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.get_key() == other.get_key()
    }
}
impl<K: Ord + Clone, V: Clone> Eq for Query<K, V> {}
