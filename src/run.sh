#!/bin/bash
BATCH_SIZE=5000
NUM_BATCHES=10000
NUM_THREADS=(1 2 4 6 8 10 12 14 16)
REPETITION=3

for p in ${NUM_THREADS[@]};
do
    echo thread=$p
    path=measure/p$p.txt;
    touch $path
    for i in $(seq 1 $REPETITION);
    do
        cargo run --release -- $p $BATCH_SIZE $NUM_BATCHES >> $path
    done
done
