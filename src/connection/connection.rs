use std::{fmt::Debug, future::Future, pin::Pin, time::Duration};

use actix::{dev::ToEnvelope, prelude::*, Message as ActixMessage};
use futures::future::{AbortHandle, Abortable};
use maxwell_protocol::{self, HandleError, ProtocolMsg};
use tokio::time::timeout;

pub trait Connection: Actor {}

#[derive(Debug, Clone)]
pub struct Options {
  pub reconnect_delay: u32,
  pub hop_interval: u32,
  pub max_idle_hops: u32,
  pub mailbox_capacity: u32,
  pub max_frame_size: u32,
}

impl Default for Options {
  fn default() -> Self {
    Options {
      reconnect_delay: 1000,
      hop_interval: 1000,
      max_idle_hops: 60,
      mailbox_capacity: 10240,
      max_frame_size: 134217728,
    }
  }
}

pub struct OptionsBuilder {
  options: Options,
}

impl OptionsBuilder {
  pub fn new() -> Self {
    OptionsBuilder { options: Options::default() }
  }

  pub fn reconnect_delay(mut self, reconnect_delay: u32) -> Self {
    self.options.reconnect_delay = reconnect_delay;
    self
  }

  pub fn hop_interval(mut self, hop_interval: u32) -> Self {
    self.options.hop_interval = hop_interval;
    self
  }

  pub fn max_idle_hops(mut self, max_idle_hops: u32) -> Self {
    self.options.max_idle_hops = max_idle_hops;
    self
  }

  pub fn mailbox_capacity(mut self, mailbox_capacity: u32) -> Self {
    self.options.mailbox_capacity = mailbox_capacity;
    self
  }

  pub fn max_frame_size(mut self, max_frame_size: u32) -> Self {
    self.options.max_frame_size = max_frame_size;
    self
  }

  pub fn build(&self) -> Options {
    self.options.clone()
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub(crate) struct HopMsg;

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct StopMsg;

pub trait TimeoutExt {
  type Result;

  fn timeout_ext(self, dur: Duration) -> Self::Result;
}

impl<C: Connection + Handler<ProtocolMsg>> TimeoutExt for Request<C, ProtocolMsg>
where <C as Actor>::Context: ToEnvelope<C, ProtocolMsg>
{
  type Result = Pin<Box<dyn Future<Output = Result<ProtocolMsg, HandleError<ProtocolMsg>>> + Send>>;

  fn timeout_ext(self, dur: Duration) -> Self::Result {
    Box::pin(async move {
      let (abort_handle, abort_registration) = AbortHandle::new_pair();
      let res = timeout(dur, Abortable::new(self, abort_registration)).await;
      match res {
        Ok(res) => match res {
          Ok(res) => match res {
            Ok(res) => res,
            Err(err) => match err {
              MailboxError::Timeout => Err(HandleError::Timeout),
              MailboxError::Closed => Err(HandleError::MailboxClosed),
            },
          },
          // Aborted
          Err(_err) => Err(HandleError::Timeout),
        },
        // Elapsed(())
        Err(_err) => {
          abort_handle.abort();
          Err(HandleError::Timeout)
        }
      }
    })
  }
}

pub const MAX_MSG_REF: u32 = 2100000000;
