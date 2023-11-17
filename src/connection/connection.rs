use std::{fmt::Debug, future::Future, pin::Pin, time::Duration};

use actix::{dev::ToEnvelope, prelude::*, Message as ActixMessage};
use futures::future::{AbortHandle, Abortable};
use maxwell_protocol::{self, HandleError, ProtocolMsg};
use tokio::time::timeout;

pub trait Connection: Actor + Handler<StopMsg> {}

#[derive(Debug, Clone)]
pub struct ConnectionOptions {
  pub reconnect_delay: u32,
  pub mailbox_capacity: u32,
  pub max_frame_size: u32,
}

impl Default for ConnectionOptions {
  fn default() -> Self {
    ConnectionOptions { reconnect_delay: 1000, mailbox_capacity: 10240, max_frame_size: 134217728 }
  }
}

pub struct ConnectionOptionsBuilder {
  options: ConnectionOptions,
}

impl ConnectionOptionsBuilder {
  pub fn new() -> Self {
    ConnectionOptionsBuilder { options: ConnectionOptions::default() }
  }

  pub fn reconnect_delay(mut self, reconnect_delay: u32) -> Self {
    self.options.reconnect_delay = reconnect_delay;
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

  pub fn build(&self) -> ConnectionOptions {
    self.options.clone()
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct StopMsg;

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct DumpInfoMsg;

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
