// port from bplustreebaseline
use super::node::Node;
use super::nodeptr::NodePtr;

pub trait RawPointerOps {
    type Output;

    fn get<'a>(self) -> &'a Self::Output;
    fn get_mut<'a>(self) -> &'a mut Self::Output;
}

impl<T> RawPointerOps for *mut T {
    type Output = T;

    fn get<'a>(self) -> &'a T {
        unsafe { &*self }
    }

    fn get_mut<'a>(self) -> &'a mut T {
        unsafe { &mut *self }
    }
}

impl<K, V> RawPointerOps for NodePtr<K, V> {
    type Output = Node<K, V>;

    #[must_use]
    fn get<'a>(self) -> &'a Self::Output {
        unsafe {
            let Self(ptr) = self;
            &*ptr
        }
    }

    #[must_use]
    fn get_mut<'a>(self) -> &'a mut Self::Output {
        unsafe {
            let Self(ptr) = self;
            &mut *ptr
        }
    }
}

pub trait SortedSearch<T> {
    fn lower_bound(&self, value: &T) -> usize;
    fn upper_bound(&self, value: &T) -> usize;
}

impl<T: Ord> SortedSearch<T> for [T] {
    default fn lower_bound(&self, value: &T) -> usize {
        // invariants: [0, l) < value & value <= [r, len)
        unsafe {
            std::intrinsics::prefetch_read_data(&self, 2);
        }
        let mut l = 0;
        let mut r = self.len();
        while l < r {
            let mid = (l + r) / 2;
            if self[mid] < *value {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        l
    }

    default fn upper_bound(&self, value: &T) -> usize {
        // invariants: [0, l) < value & value <= [r, len)
        unsafe {
            std::intrinsics::prefetch_read_data(&self, 2);
        }
        let mut l = 0;
        let mut r = self.len();
        while l < r {
            let mid = (l + r) / 2;
            if self[mid] <= *value {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        l
    }
}

impl SortedSearch<i32> for [i32] {
    #[must_use]
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        all(target_feature = "avx", target_feature = "avx2")
    ))]
    fn lower_bound(&self, value: &i32) -> usize {
        use std::arch::x86_64::*;
        let rounded = (self.len() / 8) * 8;
        unsafe {
            let value = _mm256_set1_epi32(*value);
            for i in (0..rounded).step_by(8) {
                let addr = &self[i] as *const i32 as *const __m256i;
                let vec: __m256i = _mm256_loadu_si256(addr);
                let cmp: __m256i = _mm256_cmpgt_epi32(value, vec);
                let mask = _mm256_movemask_epi8(cmp);
                if mask != -1 {
                    // 0xffffffff
                    return i + (std::intrinsics::cttz(!mask) / 4) as usize;
                }
            }
        }
        for (i, ele) in self.iter().enumerate().skip(rounded) {
            if ele >= value {
                return i;
            }
        }
        self.len()
    }

    #[must_use]
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        all(target_feature = "avx", target_feature = "avx2")
    ))]
    fn upper_bound(&self, value: &i32) -> usize {
        use std::arch::x86_64::*;
        let rounded = (self.len() / 8) * 8;
        unsafe {
            let value = _mm256_set1_epi32(*value);
            for i in (0..rounded).step_by(8) {
                let addr = &self[i] as *const i32 as *const __m256i;
                let vec: __m256i = _mm256_loadu_si256(addr);
                let cmp: __m256i = _mm256_cmpgt_epi32(vec, value);
                let mask = _mm256_movemask_epi8(cmp);
                if mask != 0 {
                    return i + (std::intrinsics::cttz(mask) / 4) as usize;
                }
            }
        }
        for (i, ele) in self.iter().enumerate().skip(rounded) {
            if ele > value {
                return i;
            }
        }
        self.len()
    }
}

pub trait LinearSearch<T> {
    fn linear_search(&self, value: &T) -> usize;
}

impl<T: Ord> LinearSearch<T> for [T] {
    default fn linear_search(&self, key: &T) -> usize {
        let mut idx = 0;
        while idx < self.len() {
            if key == &self[idx] {
                return idx;
            }
            idx += 1;
        }
        idx
    }
}

impl LinearSearch<i32> for [i32] {
    #[must_use]
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        all(target_feature = "avx", target_feature = "avx2")
    ))]
    fn linear_search(&self, value: &i32) -> usize {
        use std::arch::x86_64::*;
        unsafe {
            std::intrinsics::prefetch_read_data(&self, 2);
        }
        let rounded = (self.len() / 8) * 8;
        unsafe {
            let value = _mm256_set1_epi32(*value);
            for i in (0..rounded).step_by(8) {
                let addr = &self[i] as *const i32 as *const __m256i;
                let vec: __m256i = _mm256_loadu_si256(addr);
                let cmp: __m256i = _mm256_cmpeq_epi32(vec, value);
                let mask = _mm256_movemask_epi8(cmp);
                if mask != 0 {
                    return i + (std::intrinsics::cttz(mask) / 4) as usize;
                }
            }
        }
        for (i, ele) in self.iter().enumerate().skip(rounded) {
            if ele == value {
                return i;
            }
        }
        self.len()
    }
}
