use std::sync::Arc;

use actix::prelude::*;
use ahash::RandomState as AHasher;
use dashmap::DashMap;

use super::Connection;

#[derive(Debug, Clone)]
pub struct Options {
  pub slot_size: u8,
}

impl Default for Options {
  fn default() -> Self {
    Options { slot_size: 8 }
  }
}

pub struct ConnectionSlot<C: Connection> {
  endpoint: String,
  connections: Vec<Arc<Addr<C>>>,
  index_seed: u16,
}

impl<C: Connection> ConnectionSlot<C> {
  #[inline]
  pub fn new<F>(endpoint: String, options: &Options, init_connection: &F) -> Self
  where F: Fn(&String) -> Addr<C> {
    let mut connections = Vec::<Arc<Addr<C>>>::new();
    for _ in 0..options.slot_size {
      connections.push(Arc::new(init_connection(&endpoint)));
    }
    ConnectionSlot { endpoint, connections, index_seed: 0 }
  }

  #[inline]
  pub fn get_or_init<F>(&mut self, init_connection: &F) -> Arc<Addr<C>>
  where F: Fn(&String) -> Addr<C> {
    self.index_seed += 1;
    let index = (self.index_seed % self.connections.len() as u16) as usize;
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
  #[inline]
  pub fn new(options: Options) -> Self {
    ConnectionPool { options, slots: DashMap::with_capacity_and_hasher(512, AHasher::new()) }
  }

  #[inline]
  pub fn get_or_init<S, F>(&self, endpoint: S, init_connection: &F) -> Arc<Addr<C>>
  where
    S: AsRef<str>,
    F: Fn(&String) -> Addr<C>, {
    self
      .slots
      .entry(endpoint.as_ref().to_owned())
      .or_insert_with(|| {
        ConnectionSlot::new(endpoint.as_ref().to_owned(), &self.options, init_connection)
      })
      .value_mut()
      .get_or_init(init_connection)
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
    let connection_pool: ConnectionPool<FutureStyleConnection> =
      ConnectionPool::new(PoolOptions::default());
    let endpoint = "localhost:8081";
    let mut connections: Vec<Arc<Addr<FutureStyleConnection>>> = Vec::new();
    let start = Instant::now();
    for _i in 0..32 {
      let connection = connection_pool.get_or_init(&endpoint, &|endpoint| {
        FutureStyleConnection::start2(endpoint.to_owned(), ConnectionOptions::default())
      });
      connections.push(connection);
    }
    sleep(Duration::from_secs(3)).await;
    let spent = Instant::now() - start;
    println!("Spent {:?}ms to create connetion pool", spent.as_millis());
  }
}
