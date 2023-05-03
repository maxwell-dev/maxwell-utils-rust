use std::sync::Arc;

use actix::prelude::*;
use ahash::RandomState as AHasher;
use dashmap::DashMap;

use super::{
  connection::Connection,
  connection_pool::{ConnectionPool, Options as PoolOptions},
};

pub struct ConnectionMgr {
  pool_options: PoolOptions,
  pools: DashMap<String, ConnectionPool, AHasher>,
}

impl ConnectionMgr {
  pub fn new(pool_options: PoolOptions) -> Self {
    ConnectionMgr { pool_options, pools: DashMap::with_capacity_and_hasher(512, AHasher::new()) }
  }

  #[inline]
  pub fn fetch_connection(&self, endpoint: &str) -> Arc<Addr<Connection>> {
    self
      .pools
      .entry(endpoint.to_owned())
      .or_insert_with(|| ConnectionPool::new(endpoint.to_owned(), self.pool_options))
      .value_mut()
      .fetch_connection()
  }
}

impl Default for ConnectionMgr {
  fn default() -> Self {
    ConnectionMgr::new(PoolOptions::default())
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
  use maxwell_protocol::{IntoEnum, PingReq};
  use tokio::time::sleep;

  use crate::connection::{Connection, ConnectionMgr};

  #[actix::test]
  async fn fetch_connection() {
    let connection_mgr = ConnectionMgr::default();
    let endpoint = "localhost:8081";
    let mut connections: Vec<Arc<Addr<Connection>>> = Vec::new();
    let start = Instant::now();
    for _i in 0..32 {
      let connection = connection_mgr.fetch_connection(&endpoint);
      connection.send(PingReq { r#ref: 1 }.into_enum()).await.unwrap().unwrap();
      connections.push(connection);
    }
    sleep(Duration::from_secs(3)).await;
    let spent = Instant::now() - start;
    println!("Spent time: {:?}ms", spent.as_millis());
  }
}
