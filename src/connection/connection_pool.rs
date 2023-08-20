use std::sync::Arc;

use actix::prelude::*;
use ahash::RandomState as AHasher;
use dashmap::DashMap;

use super::Connection;

#[derive(Debug, Clone)]
pub struct ConnectionPoolOptions {
  pub slot_size: u8,
}

impl Default for ConnectionPoolOptions {
  fn default() -> Self {
    ConnectionPoolOptions { slot_size: 8 }
  }
}

pub struct ConnectionSlot<C: Connection> {
  endpoint: String,
  connections: Vec<Arc<Addr<C>>>,
  index_seed: u16,
}

impl<C: Connection> ConnectionSlot<C> {
  #[inline]
  pub fn new<F>(endpoint: String, options: &ConnectionPoolOptions, init_connection: &F) -> Self
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
    let index = self.next_index();
    let connection = &self.connections[index];
    if connection.connected() {
      Arc::clone(connection)
    } else {
      self.connections[index] = Arc::new(init_connection(&self.endpoint));
      Arc::clone(&self.connections[index])
    }
  }

  #[inline]
  pub fn remove(&mut self, connection: &Arc<Addr<C>>) {
    let index = self.connections.iter().position(|c| c == connection);
    if let Some(index) = index {
      self.connections.swap_remove(index);
    }
  }

  #[inline]
  pub fn next_index(&mut self) -> usize {
    if self.index_seed as u32 + 1 > u16::MAX as u32 {
      self.index_seed = 0;
    } else {
      self.index_seed += 1;
    }
    (self.index_seed % self.connections.len() as u16) as usize
  }
}

pub struct ConnectionPool<C: Connection> {
  options: ConnectionPoolOptions,
  slots: DashMap<String, ConnectionSlot<C>, AHasher>,
}

impl<C: Connection> ConnectionPool<C> {
  #[inline]
  pub fn new(options: ConnectionPoolOptions) -> Self {
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

  #[inline]
  pub fn remove<S>(&self, endpoint: S, connection: &Arc<Addr<C>>)
  where S: AsRef<str> {
    self.slots.get_mut(endpoint.as_ref()).map(|mut slot| slot.remove(connection));
  }

  #[inline]
  pub fn remove_by_endpoint<S>(&self, endpoint: S)
  where S: AsRef<str> {
    self.slots.remove(endpoint.as_ref());
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
      ConnectionPool::new(ConnectionPoolOptions::default());
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

    let connection = connection_pool.get_or_init(endpoint, &|endpoint| {
      FutureStyleConnection::start2(endpoint.to_owned(), ConnectionOptions::default())
    });
    connection_pool.remove(endpoint, &connection);

    let connection = connection_pool.get_or_init(endpoint, &|endpoint| {
      FutureStyleConnection::start2(endpoint.to_owned(), ConnectionOptions::default())
    });
    connection_pool.remove(endpoint, &connection);
  }
}
