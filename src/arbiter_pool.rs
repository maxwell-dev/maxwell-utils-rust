use actix::prelude::*;
use dycovec::DycoVec;
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicU8, Ordering};

static ARBITER_POOL: OnceCell<ArbiterPool> = OnceCell::new();

pub struct ArbiterPool {
  arbiters: DycoVec<Arbiter>,
  len: u8,
  index_seed: AtomicU8,
}

impl ArbiterPool {
  pub fn singleton() -> &'static Self {
    ARBITER_POOL.get_or_init(|| {
      let arbiters = DycoVec::<Arbiter>::new();
      let len = Self::get_cpu_count();
      for _ in 0..len {
        arbiters.push(Arbiter::new());
      }
      ArbiterPool { arbiters, len, index_seed: AtomicU8::new(0) }
    })
  }

  pub fn fetch_arbiter(&self) -> ArbiterHandle {
    let index_seed = self.index_seed.fetch_add(1, Ordering::Relaxed);
    let index = (index_seed % self.len) as usize;
    self.arbiters[index].handle()
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
