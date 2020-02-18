use super::nodeptr::NodePtr;
use super::notthreadsafe::NotThreadSafe;
use super::query::Query;
use super::tree::*;
use std::collections::VecDeque;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Barrier,
};
use std::thread;

pub struct Worker<K, V>
where
    K: Ord + Clone + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
{
    thread_index: usize,
    tree: Arc<NotThreadSafe<Palm<K, V>>>,
    barrier: Arc<Barrier>,
    q_query: Arc<Vec<Vec<NotThreadSafe<QueryMap<K, V>>>>>,
    q_modif: Arc<Vec<Vec<NotThreadSafe<ModifMap<K, V>>>>>,
    first: Arc<NotThreadSafe<Vec<Vec<Option<NodePtr<K, V>>>>>>,
    last: Arc<NotThreadSafe<Vec<Vec<Option<NodePtr<K, V>>>>>>,
    their_first: NotThreadSafe<NodePtr<K, V>>,
    their_last: NotThreadSafe<NodePtr<K, V>>,
}

impl<K, V> Worker<K, V>
where
    K: 'static + Ord + Clone + std::fmt::Debug + Sync + Send,
    V: 'static + Clone + std::fmt::Debug + Sync + Send,
{
    #[must_use]
    pub fn new(
        thread_index: usize,
        tree: Arc<NotThreadSafe<Palm<K, V>>>,
        barrier: Arc<Barrier>,
        q_query: Arc<Vec<Vec<NotThreadSafe<QueryMap<K, V>>>>>,
        q_modif: Arc<Vec<Vec<NotThreadSafe<ModifMap<K, V>>>>>,
        first: Arc<NotThreadSafe<Vec<Vec<Option<NodePtr<K, V>>>>>>,
        last: Arc<NotThreadSafe<Vec<Vec<Option<NodePtr<K, V>>>>>>,
    ) -> Self {
        Self {
            thread_index,
            tree,
            barrier,
            q_query,
            q_modif,
            first,
            last,
            their_first: NotThreadSafe::new(NodePtr::new(std::ptr::null_mut())),
            their_last: NotThreadSafe::new(NodePtr::new(std::ptr::null_mut())),
        }
    }

    fn global_sync(&self) {
        self.barrier.wait();
    }

    pub fn point_to_point_sync<T: std::fmt::Debug + Clone>(
        &self,
        depth: usize,
        input: &[NotThreadSafe<VecDeque<(NodePtr<K, V>, Vec<T>)>>],
        first: &mut [Vec<Option<NodePtr<K, V>>>],
        last: &mut [Vec<Option<NodePtr<K, V>>>],
        num_threads: usize,
    ) {
        let cur_layer = input[self.thread_index].get();

        let mut my_first = cur_layer.front().map(|x| x.0);
        let mut my_last = cur_layer.back().map(|x| x.0);
        if my_first == my_last {
            // if thread i has only one node,
            //   then thread (i-1) probably also wants thread (i+1)'s
            //   first node
            // => By setting thread i's `my_first` to None, thread (i-1)
            //   will fetch `my_first` from thread (i+1) instead
            my_first = None;
        }
        assert_eq!(first[self.thread_index].len(), depth);
        assert_eq!(last[self.thread_index].len(), depth);

        let mut their_first = None;
        let mut their_last = None;
        let mut send_first = false;
        let mut send_last = false;
        while their_first.is_none() || their_last.is_none() || !send_first || !send_last {
            if my_first.is_some() && !send_first {
                // send my_first to i-1
                first[self.thread_index].push(my_first);
                send_first = true;
            }
            if my_last.is_some() && !send_last {
                // send my_last to i+1
                last[self.thread_index].push(my_last);
                send_last = true;
            }
            if their_first.is_none() {
                // fetch their first from i+1
                if self.thread_index >= num_threads - 1 {
                    their_first = Some(NodePtr::new(std::ptr::null_mut()));
                } else if first[self.thread_index + 1].len() > depth {
                    their_first = first[self.thread_index + 1][depth];
                }
            }
            if their_first.is_some() && my_first.is_none() {
                my_first = their_first;
            }
            if their_last.is_none() {
                // fetch their last from i-1
                if self.thread_index == 0 {
                    their_last = Some(NodePtr::new(std::ptr::null_mut()));
                } else if last[self.thread_index - 1].len() > depth {
                    their_last = last[self.thread_index - 1][depth];
                }
            }
            if their_last.is_some() && my_last.is_none() {
                my_last = their_last;
            }
        }
        std::mem::replace(self.their_first.get_mut(), their_first.unwrap());
        std::mem::replace(self.their_last.get_mut(), their_last.unwrap());
    }

    pub fn execute(&self, mut queries: Vec<Query<K, V>>) -> Vec<(Query<K, V>, Option<V>)> {
        let depth = self.tree.get().depth;
        let num_threads = self.tree.get().num_threads;
        self.first.get_mut()[self.thread_index].clear();
        self.last.get_mut()[self.thread_index].clear();
        // Stage 1:
        //   1. divide tree queries among threads
        //   2. independently search for leaves for each query
        Palm::search(
            &mut queries,
            &self.q_query[0][self.thread_index],
            self.tree.get().root,
        );
        self.global_sync();

        // Stage 2:
        //   1. redistribute work to ensure no modification
        //     contention, and ensure ordering of queries
        //   2. modify leaves independently
        assert!(self.their_first.get_mut().is_null());
        assert!(self.their_last.get_mut().is_null());
        Palm::redistribute_work(
            self.thread_index,
            &self.q_query[0],
            num_threads,
            self.their_last.get_mut(),
        );
        let responses = Palm::apply_to_leaf_nodes(
            &self.q_query[0][self.thread_index],
            &self.q_modif[0][self.thread_index],
            *self.their_last.get_mut(),
        );
        self.point_to_point_sync(
            0,
            &self.q_modif[0],
            self.first.get_mut(),
            self.last.get_mut(),
            num_threads,
        );

        // Stage 3:
        //   1. proceed in 'lock-step' up the tree, modify
        //     internal nodes and redistributing the works,
        //     up to the root
        let mut level_ptr = 0;
        for d in 1..depth {
            Palm::redistribute_work(
                self.thread_index,
                &self.q_modif[level_ptr],
                num_threads,
                self.their_last.get_mut(),
            );
            Palm::apply_to_internal_nodes(
                &self.q_modif[level_ptr][self.thread_index],
                &self.q_modif[(level_ptr + 1) % 2][self.thread_index],
                *self.their_last.get_mut(),
            );
            level_ptr = (level_ptr + 1) % 2;
            self.point_to_point_sync(
                d,
                &self.q_modif[level_ptr],
                self.first.get_mut(),
                self.last.get_mut(),
                num_threads,
            );
        }

        // Stage 4:
        //   1. a single thread modifies the root
        //   2. (potentionally) change the depth of tree
        if self.thread_index == 0 {
            // handle the root
            Palm::handle_root(&self.tree, &self.q_modif[level_ptr]);
        }
        responses
    }

    pub fn start(
        self,
    ) -> (
        thread::JoinHandle<()>,
        Sender<Message<K, V>>,
        Receiver<Vec<(Query<K, V>, Option<V>)>>,
    ) {
        let (in_sender, in_receiver) = channel();
        let (out_sender, out_receiver) = channel();

        let sender = out_sender;
        let receiver = in_receiver;

        let handle = thread::spawn(move || loop {
            let msg = receiver.recv().unwrap();
            match msg {
                Message::Query(queries) => {
                    let resp = self.execute(queries);
                    sender.send(resp).unwrap();
                }
                Message::Terminate => {
                    break;
                }
            }
        });
        (handle, in_sender, out_receiver)
    }
}

pub enum Message<K, V> {
    Query(Vec<Query<K, V>>),
    Terminate,
}

pub struct PalmWrapper<K, V> {
    pub seq_time: u128,
    pub par_time: u128,

    num_threads: usize,

    handles: Vec<std::thread::JoinHandle<()>>,
    senders: Vec<Sender<Message<K, V>>>,
    receivers: Vec<Receiver<Vec<(Query<K, V>, Option<V>)>>>,
}

impl<K, V> PalmWrapper<K, V>
where
    K: 'static + Ord + Clone + std::fmt::Debug + Sync + Send,
    V: 'static + Clone + std::fmt::Debug + Sync + Send,
{
    #[must_use]
    pub fn new(tree: Arc<NotThreadSafe<Palm<K, V>>>, num_threads: usize) -> Self {
        let barrier = Arc::new(Barrier::new(num_threads));
        let q_query: Arc<Vec<_>> = Arc::new(
            (0..2)
                .map(|_| {
                    (0..num_threads)
                        .map(|_| NotThreadSafe::new(VecDeque::new()))
                        .collect()
                })
                .collect(),
        );
        let q_modif: Arc<Vec<_>> = Arc::new(
            (0..2)
                .map(|_| {
                    (0..num_threads)
                        .map(|_| NotThreadSafe::new(VecDeque::new()))
                        .collect()
                })
                .collect(),
        );
        let first: Arc<NotThreadSafe<Vec<_>>> = Arc::new(NotThreadSafe::new(
            (0..num_threads).map(|_| Vec::new()).collect(),
        ));
        let last: Arc<NotThreadSafe<Vec<_>>> = Arc::new(NotThreadSafe::new(
            (0..num_threads).map(|_| Vec::new()).collect(),
        ));

        let mut handles = Vec::new();
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for i in 0..num_threads {
            let worker = Worker::new(
                i,
                tree.clone(),
                barrier.clone(),
                q_modif.clone(),
                q_query.clone(),
                first.clone(),
                last.clone(),
            );
            let (handle, sender, receiver) = worker.start();
            handles.push(handle);
            senders.push(sender);
            receivers.push(receiver);
        }

        Self {
            seq_time: 0,
            par_time: 0,
            num_threads,
            handles,
            senders,
            receivers,
        }
    }

    pub fn partition<T: Clone>(batch: &[T], t: usize) -> Vec<Vec<T>> {
        batch
            .chunks((batch.len() + t - 1) / t)
            .map(|x| x.to_vec())
            .collect()
    }

    pub fn run_batch(&mut self, queries: &mut Vec<Query<K, V>>) -> Vec<(Query<K, V>, Option<V>)> {
        // by sorting in advance, redistribution can be significantly simplified
        //   note that sort has to be stable to preserve the order of queries
        let now = std::time::Instant::now();
        queries.sort();
        let mut partitions = Self::partition(&queries, self.num_threads);
        self.seq_time += now.elapsed().as_micros();

        let now = std::time::Instant::now();
        for (i, queries) in partitions.drain(..).enumerate() {
            self.senders[i].send(Message::Query(queries)).unwrap();
        }
        let mut results = Vec::new();
        for i in 0..self.num_threads {
            results.extend(self.receivers[i].recv().unwrap());
        }
        self.par_time += now.elapsed().as_micros();
        results
    }
}

impl<K, V> Drop for PalmWrapper<K, V> {
    fn drop(&mut self) {
        for i in 0..self.num_threads {
            self.senders[i].send(Message::Terminate).unwrap();
        }

        for handle in self.handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
