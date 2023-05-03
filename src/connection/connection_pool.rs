use std::sync::{
  atomic::{AtomicU8, Ordering},
  Arc,
};

use actix::prelude::*;
use dycovec::DycoVec;

use crate::connection::Connection;

#[derive(Debug, Copy, Clone)]
pub struct Options {
  size: u8,
  //   min_size: u8,
  //   max_size: u8,
}

impl Default for Options {
  fn default() -> Self {
    Options { size: 8 /*min_size: 1, max_size: 16*/ }
  }
}

pub struct ConnectionPool {
  endpoint: String,
  connections: DycoVec<Arc<Addr<Connection>>>,
  index_seed: AtomicU8,
}

impl ConnectionPool {
  pub fn new(endpoint: String, options: Options) -> Self {
    let connections = DycoVec::<Arc<Addr<Connection>>>::new();
    for _ in 0..options.size {
      connections.push(Arc::new(Connection::start_with_endpoint(endpoint.clone())));
    }
    ConnectionPool { endpoint, connections, index_seed: AtomicU8::new(0) }
  }

  #[inline]
  pub fn fetch_connection(&mut self) -> Arc<Addr<Connection>> {
    let index_seed = self.index_seed.fetch_add(1, Ordering::Relaxed);
    let index = (index_seed % self.connections.len() as u8) as usize;
    let connection = &self.connections[index];
    if connection.connected() {
      Arc::clone(connection)
    } else {
      self.connections[index] = Arc::new(Connection::start_with_endpoint(self.endpoint.clone()));
      Arc::clone(&self.connections[index])
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use std::{
    sync::atomic::{AtomicU8, Ordering},
    time::Instant,
  };

  #[actix::test]
  async fn test_fetch_add() {
    let val = AtomicU8::new(0);
    let start = Instant::now();
    for _i in 0..258 {
      let next = val.fetch_add(1, Ordering::Relaxed);
      println!("next: {:?}", next);
    }
    let spent = Instant::now() - start;
    println!("Spent time: {:?}ms", spent.as_millis());
  }
}
