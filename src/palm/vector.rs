use super::node::MAX_LEN;
use std::mem::MaybeUninit;
use std::ops::{Index, IndexMut};

const MAX_CAPACITY: usize = MAX_LEN + 1;

#[derive(Clone)]
#[repr(C)]
pub struct MyVector<K> {
    len: usize,
    data: [K; MAX_CAPACITY],
}

impl<T> MyVector<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            len: 0,
            data: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, value: T) {
        assert!(self.len < MAX_CAPACITY);
        self.data[self.len] = value;
        self.len += 1;
    }

    pub fn insert(&mut self, k: usize, v: T) {
        assert!(self.len < MAX_CAPACITY);
        assert!(k <= self.len());
        self.len += 1;
        for i in (k + 1..self.len).rev() {
            self.data.swap(i, i - 1);
        }
        self.data[k] = v;
    }

    pub fn split_off(&mut self, size: usize) -> Self {
        assert_eq!(self.len() >= size, true);
        let mut vec = Self::new();
        for (count, idx) in ((self.len - size)..self.len).enumerate() {
            std::mem::swap(&mut vec[count], &mut self.data[idx]);
        }
        self.len -= size;
        vec.len = size;
        vec
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            self.len -= 1;
            Some(std::mem::replace(&mut self.data[self.len], unsafe {
                std::mem::MaybeUninit::uninit().assume_init()
            }))
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn last(&self) -> Option<&T> {
        if self.len > 1 {
            Some(&self.data[self.len - 1])
        } else {
            None
        }
    }
}

impl<T: Clone> MyVector<T> {
    pub fn clone_from_vec(&mut self, vec: &mut Vec<T>) {
        assert!(vec.len() <= MAX_CAPACITY);
        self.len = 0;
        for data in vec.drain(..) {
            self.push(data);
        }
    }
}

impl<T> std::ops::Deref for MyVector<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(&self.data[0] as *const _, self.len) }
    }
}

impl<T> std::ops::DerefMut for MyVector<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(&mut self.data[0] as *mut _, self.len) }
    }
}

impl<'a, K> IntoIterator for &'a MyVector<K> {
    type Item = &'a K;
    type IntoIter = std::slice::Iter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K> IntoIterator for &'a mut MyVector<K> {
    type Item = &'a mut K;
    type IntoIter = std::slice::IterMut<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<K> Index<usize> for MyVector<K> {
    type Output = K;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<K> IndexMut<usize> for MyVector<K> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

impl<K: std::fmt::Debug> std::fmt::Debug for MyVector<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = "MyVector:: [ ".to_string();
        for i in 0..self.len() {
            s += &format!("{:?} ", self.data[i]);
        }
        write!(f, "{}]", s)
    }
}

impl<T: Clone> From<Vec<T>> for MyVector<T> {
    #[must_use]
    fn from(vec: Vec<T>) -> Self {
        assert!(vec.len() <= MAX_CAPACITY);
        let mut myvec = Self::new();
        myvec.len = vec.len();
        myvec.clone_from_slice(&vec);
        myvec
    }
}

impl<T: Clone> Into<Vec<T>> for MyVector<T> {
    fn into(self) -> Vec<T> {
        self.to_vec()
    }
}

#[derive(Debug)]
enum MyVectorError {}
