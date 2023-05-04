use std::time::Duration;

use actix::{prelude::*, Message as ActixMessage};

trait Connection: Actor {}

#[derive(Debug, Clone)]
pub struct Options {
  pub reconnect_delay: u32,
  pub ping_interval: Option<u32>,
}

impl Default for Options {
  fn default() -> Self {
    Options { reconnect_delay: 1000, ping_interval: Some(1000) }
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "u32")]
pub struct NextMsgRefMsg;

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct StopMsg;

pub trait TimeoutExt {
  type Result;

  fn timeout_ext(self, dur: Duration) -> Self::Result;
}

pub const MAX_MSG_REF: u32 = 2100000000;
