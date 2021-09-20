use actix::prelude::*;
use dycovec::DycoVec;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

pub struct ArbiterPool {
    arbiters: DycoVec<Arc<Arbiter>>,
    index_seed: AtomicU8,
    cpu_count: u8,
}

impl ArbiterPool {
    pub fn new() -> Self {
        let arbiters = DycoVec::<Arc<Arbiter>>::new();
        let cpu_count = Self::get_cpu_count();
        for _ in 0..cpu_count {
            arbiters.push(Arc::new(Arbiter::new()));
        }
        ArbiterPool { arbiters, index_seed: AtomicU8::new(0), cpu_count }
    }

    pub fn fetch_arbiter(&self) -> Arc<Arbiter> {
        let index_seed = self.index_seed.fetch_add(1, Ordering::Relaxed);
        let index = (index_seed % self.cpu_count) as usize;
        Arc::clone(&self.arbiters[index])
    }

    fn get_cpu_count() -> u8 {
        let cpu_count = num_cpus::get();
        if cpu_count < u8::MAX as usize {
            cpu_count as u8
        } else {
            u8::MAX
        }
    }
}
