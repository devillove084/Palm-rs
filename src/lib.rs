#![feature(specialization)]
#![feature(core_intrinsics)]
#![feature(vec_into_raw_parts)]
// #![feature(const_generics)]

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub mod palm;
