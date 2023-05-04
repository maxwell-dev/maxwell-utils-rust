use std::{future::Future, pin::Pin, time::Duration};

use actix::{dev::ToEnvelope, prelude::*, Message as ActixMessage};
use futures::future::{AbortHandle, Abortable};
use maxwell_protocol::{self, ProtocolMsg, SendError};
use tokio::time::timeout;

pub trait Connection: Actor {}

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

impl<C: Connection + Handler<ProtocolMsg>> TimeoutExt for Request<C, ProtocolMsg>
where <C as Actor>::Context: ToEnvelope<C, ProtocolMsg>
{
  type Result = Pin<Box<dyn Future<Output = Result<ProtocolMsg, SendError>> + Send>>;

  fn timeout_ext(self, dur: Duration) -> Self::Result {
    Box::pin(async move {
      let (abort_handle, abort_registration) = AbortHandle::new_pair();
      let res = timeout(dur, Abortable::new(self, abort_registration)).await;
      match res {
        Ok(res) => match res {
          Ok(res) => match res {
            Ok(res) => match res {
              Ok(res) => Ok(res),
              Err(err) => Err(err),
            },
            Err(err) => match err {
              MailboxError::Timeout => Err(SendError::Timeout),
              MailboxError::Closed => Err(SendError::Closed),
            },
          },
          // Aborted
          Err(_err) => Err(SendError::Timeout),
        },
        // Elapsed(())
        Err(_err) => {
          abort_handle.abort();
          Err(SendError::Timeout)
        }
      }
    })
  }
}

pub const MAX_MSG_REF: u32 = 2100000000;
