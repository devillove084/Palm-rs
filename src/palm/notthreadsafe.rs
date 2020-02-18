use std::cell::UnsafeCell;

pub struct NotThreadSafe<T> {
    data: UnsafeCell<T>,
}

impl<T> NotThreadSafe<T> {
    pub fn new(data: T) -> Self {
        Self {
            data: UnsafeCell::new(data),
        }
    }

    pub fn get<'a>(&self) -> &'a T {
        unsafe { &*(self.data.get()) }
    }

    pub fn get_mut<'a>(&self) -> &'a mut T {
        unsafe { &mut *(self.data.get()) }
    }
}

unsafe impl<T> Sync for NotThreadSafe<T> {}
unsafe impl<T> Send for NotThreadSafe<T> {}
