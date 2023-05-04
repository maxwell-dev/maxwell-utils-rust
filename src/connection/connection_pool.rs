use std::sync::{
  atomic::{AtomicU8, Ordering},
  Arc,
};

use actix::prelude::*;
use ahash::RandomState as AHasher;
use dashmap::DashMap;
use dycovec::DycoVec;

use super::Connection;

#[derive(Debug, Clone)]
pub struct Options {
  initial_slot_size: u8,
  //   min_slot_size: u8,
  //   max_slot_size: u8,
}

impl Default for Options {
  fn default() -> Self {
    Options { initial_slot_size: 8 }
  }
}

pub struct ConnectionSlot<C: Connection> {
  endpoint: String,
  connections: DycoVec<Arc<Addr<C>>>,
  index_seed: AtomicU8,
}

impl<C: Connection> ConnectionSlot<C> {
  pub fn new<F>(endpoint: String, options: &Options, init_connection: &F) -> Self
  where F: Fn(&String) -> Addr<C> {
    let connections = DycoVec::<Arc<Addr<C>>>::new();
    for _ in 0..options.initial_slot_size {
      connections.push(Arc::new(init_connection(&endpoint)));
    }
    ConnectionSlot { endpoint, connections, index_seed: AtomicU8::new(0) }
  }

  #[inline]
  pub fn fetch_with<F>(&mut self, init_connection: &F) -> Arc<Addr<C>>
  where F: Fn(&String) -> Addr<C> {
    let index_seed = self.index_seed.fetch_add(1, Ordering::Relaxed);
    let index = (index_seed % self.connections.len() as u8) as usize;
    let connection = &self.connections[index];
    if connection.connected() {
      Arc::clone(connection)
    } else {
      self.connections[index] = Arc::new(init_connection(&self.endpoint));
      Arc::clone(&self.connections[index])
    }
  }
}

pub struct ConnectionPool<C: Connection> {
  options: Options,
  slots: DashMap<String, ConnectionSlot<C>, AHasher>,
}

impl<C: Connection> ConnectionPool<C> {
  pub fn new(options: Options) -> Self {
    ConnectionPool { options, slots: DashMap::with_capacity_and_hasher(512, AHasher::new()) }
  }

  #[inline]
  pub fn fetch_with<F>(&self, endpoint: &str, init_connection: &F) -> Arc<Addr<C>>
  where F: Fn(&String) -> Addr<C> {
    self
      .slots
      .entry(endpoint.to_owned())
      .or_insert_with(|| ConnectionSlot::new(endpoint.to_owned(), &self.options, init_connection))
      .value_mut()
      .fetch_with(init_connection)
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {

  use std::{
    sync::Arc,
    time::{Duration, Instant},
  };

  use actix::prelude::*;
  use tokio::time::sleep;

  use super::*;
  use crate::connection::*;

  #[actix::test]
  async fn fetch_with() {
    let connection_pool: ConnectionPool<ConnectionFull> = ConnectionPool::new(Options::default());
    let endpoint = "localhost:8081";
    let mut connections: Vec<Arc<Addr<ConnectionFull>>> = Vec::new();
    let start = Instant::now();
    for _i in 0..32 {
      let connection = connection_pool.fetch_with(&endpoint, &|endpoint| {
        ConnectionFull::start2(endpoint.to_owned(), ConnectionOptions::default())
      });
      connections.push(connection);
    }
    sleep(Duration::from_secs(3)).await;
    let spent = Instant::now() - start;
    println!("Spent {:?}ms to create connetion pool", spent.as_millis());
  }
}
