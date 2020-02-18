use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;

use palm::palm::notthreadsafe::NotThreadSafe;
use palm::palm::query::Query;
use palm::palm::tree::*;
use palm::palm::worker::*;

type KeyType = i32;
const KEY_RANGE: KeyType = 1073741824;

#[allow(non_snake_case)]
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 4 {
        println!("Usage: <NUM_THREADS> <BATCH_SIZE> <NUM_BATCHES>");
        return;
    }

    let NUM_THREADS: usize = args[1].parse().unwrap();
    let BATCH_SIZE: usize = args[2].parse().unwrap();
    let NUM_BATCHES: usize = args[3].parse().unwrap();

    let tree = Arc::new(NotThreadSafe::new(Palm::<KeyType, KeyType>::new(
        NUM_THREADS,
    )));
    let mut wrapper = PalmWrapper::new(tree, NUM_THREADS);

    let seed = [1u8; 32];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    for _ in 0..NUM_BATCHES {
        let mut queries: Vec<_> = (0..BATCH_SIZE)
            .map(|_| {
                let k = rng.gen_range(1, KEY_RANGE);
                if rng.gen::<bool>() {
                    Query::Insertion {
                        k,
                        v: rng.gen_range(1, 100),
                    }
                } else {
                    Query::Retrieval { k }
                }
            })
            .collect();
        wrapper.run_batch(&mut queries);
    }
    println!(
        "[Time] Sequential: {} μs, Parallel: {} μs",
        wrapper.seq_time, wrapper.par_time
    );
}
