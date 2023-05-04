use std::{
  cell::{Cell, RefCell},
  future::Future,
  marker::Unpin,
  pin::Pin,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use actix::{prelude::*, Addr};
use actix_codec::Framed;
use actix_web_actors::ws::{Frame, Message as WSMessage};
use awc::{ws::Codec, BoxedSocket, Client};
use bytes::Bytes;
use futures::future::{AbortHandle, Abortable};
use futures_intrusive::sync::LocalManualResetEvent;
use futures_util::{
  sink::SinkExt,
  stream::{SplitSink, SplitStream, StreamExt},
};
use maxwell_protocol::{self, ProtocolMsg, SendError, *};
use tokio::time::{sleep, timeout};

use super::Connection;
use super::ConnectionOptions;
use super::NextMsgRefMsg;
use super::StopMsg;
use super::TimeoutExt;
use super::MAX_MSG_REF;
use crate::prelude::ArbiterPool;

static ID_SEED: AtomicU32 = AtomicU32::new(0);

pub trait EventHandler: Send + Sync + Unpin + 'static {
  #[inline]
  fn on_msg(&self, _msg: ProtocolMsg) {}
  #[inline]
  fn on_connected(&self) {}
  #[inline]
  fn on_disconnected(&self) {}
  #[inline]
  fn on_stopped(&self) {}
}

struct ConnectionLiteInner<H: EventHandler> {
  id: u32,
  url: String,
  options: ConnectionOptions,
  sink: RefCell<Option<SplitSink<Framed<BoxedSocket, Codec>, WSMessage>>>,
  stream: RefCell<Option<SplitStream<Framed<BoxedSocket, Codec>>>>,
  connected_event: LocalManualResetEvent,
  disconnected_event: LocalManualResetEvent,
  is_connected: Cell<bool>,
  msg_ref: Cell<u32>,
  is_stopping: Cell<bool>,
  event_handler: H,
}

impl<H: EventHandler> ConnectionLiteInner<H> {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions, event_handler: H) -> Self {
    ConnectionLiteInner {
      id: ID_SEED.fetch_add(1, Ordering::Relaxed),
      url: Self::build_url(&endpoint),
      options,
      sink: RefCell::new(None),
      stream: RefCell::new(None),
      connected_event: LocalManualResetEvent::new(false),
      disconnected_event: LocalManualResetEvent::new(true),
      is_connected: Cell::new(false),
      msg_ref: Cell::new(1),
      is_stopping: Cell::new(false),
      event_handler,
    }
  }

  pub async fn connect_repeatedly(self: Rc<Self>) {
    loop {
      if self.is_stopping() {
        break;
      }

      self.disconnected_event.wait().await;

      log::info!("Connecting: actor: {}<{}>", &self.url, &self.id);
      match Client::new().ws(&self.url).connect().await {
        Ok((_resp, socket)) => {
          log::info!("Connected: actor: {}<{}>", &self.url, &self.id);
          let (sink, stream) = StreamExt::split(socket);
          self.set_socket_pair(Some(sink), Some(stream));
          self.toggle_to_connected();
        }
        Err(err) => {
          log::error!("Failed to connect: actor: {}<{}>, err: {}", &self.url, &self.id, err);
          self.set_socket_pair(None, None);
          self.toggle_to_disconnected();
          sleep(Duration::from_millis(self.options.reconnect_delay as u64)).await;
        }
      }
    }
  }

  pub async fn ping_repeatedly_or_stop(self: Rc<Self>) {
    loop {
      if self.is_stopping() {
        break;
      }

      if !self.is_connected() {
        self.connected_event.wait().await;
      }

      if let Err(err) =
        self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Ping(Bytes::from("!"))).await
      {
        log::error!("Failed to send ping: err: {}", &err);
      }

      sleep(Duration::from_secs(self.options.ping_interval.unwrap() as u64)).await;
    }
  }

  pub async fn send(self: Rc<Self>, mut msg: ProtocolMsg) -> Result<ProtocolMsg, SendError> {
    let mut msg_ref = maxwell_protocol::get_ref(&msg);
    if msg_ref == 0 {
      msg_ref = self.next_msg_ref();
      maxwell_protocol::set_ref(&mut msg, msg_ref);
    } else {
      if msg_ref - 1 == self.last_msg_ref() {
        return Err(SendError::DuplicatedMsgRef(msg_ref));
      }
    }

    if !self.is_connected() {
      self.connected_event.wait().await;
    }

    if let Err(err) =
      self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Binary(encode(&msg))).await
    {
      log::error!("Failed to send msg: err: {}", &err);
      return Err(SendError::Any(Box::new(err)));
    }

    Ok(ProtocolMsg::None)
  }

  pub async fn receive_repeatedly(self: Rc<Self>) {
    loop {
      if self.is_stopping() {
        break;
      }

      if !self.is_connected() {
        self.connected_event.wait().await;
      }

      if let Some(res) = self.stream.borrow_mut().as_mut().unwrap().next().await {
        match res {
          Ok(frame) => match frame {
            Frame::Binary(bytes) => {
              let msg = maxwell_protocol::decode(&bytes).unwrap();
              self.event_handler.on_msg(msg);
            }
            Frame::Ping(ping) => {
              if let Err(err) =
                self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Pong(ping)).await
              {
                log::error!("Failed to send pong: err: {}", &err);
              }
            }
            Frame::Pong(pong) => {
              if let Err(err) =
                self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Ping(pong)).await
              {
                log::error!("Failed to send ping: err: {}", &err);
              }
            }
            Frame::Close(reason) => {
              log::error!("Disconnected: actor: {}<{}>, err: {:?}", &self.url, &self.id, &reason);
              self.toggle_to_disconnected();
            }
            other => {
              log::warn!("Received unknown msg: {:?}", &other);
            }
          },
          Err(err) => {
            log::error!("Protocol error occured: err: {}", &err);
            self.toggle_to_disconnected();
          }
        }
      } else {
        log::error!(
          "Disconnected: actor: {}<{}>, err: {}",
          &self.url,
          &self.id,
          "eof of the stream"
        );
        self.toggle_to_disconnected();
      }
    }
  }

  #[inline]
  pub fn stop(&self) {
    self.is_stopping.set(true);
  }

  #[inline]
  pub fn next_msg_ref(&self) -> u32 {
    let msg_ref = self.msg_ref.get();
    if msg_ref < MAX_MSG_REF {
      self.msg_ref.set(msg_ref + 1);
    } else {
      self.msg_ref.set(1);
    }
    self.msg_ref.get()
  }

  #[inline]
  fn last_msg_ref(&self) -> u32 {
    self.msg_ref.get()
  }

  #[inline]
  fn build_url(endpoint: &str) -> String {
    format!("ws://{}/ws", endpoint)
  }

  #[inline]
  fn set_socket_pair(
    &self, sink: Option<SplitSink<Framed<BoxedSocket, Codec>, WSMessage>>,
    stream: Option<SplitStream<Framed<BoxedSocket, Codec>>>,
  ) {
    *self.sink.borrow_mut() = sink;
    *self.stream.borrow_mut() = stream;
  }

  #[inline]
  fn toggle_to_connected(&self) {
    self.is_connected.set(true);
    self.connected_event.set();
    self.disconnected_event.reset();
    self.event_handler.on_connected();
  }

  #[inline]
  fn toggle_to_disconnected(&self) {
    self.is_connected.set(false);
    self.connected_event.reset();
    self.disconnected_event.set();
    self.event_handler.on_disconnected();
  }

  #[inline]
  fn is_connected(&self) -> bool {
    self.is_connected.get()
  }

  #[inline]
  fn is_stopping(&self) -> bool {
    self.is_stopping.get()
  }
}

pub struct ConnectionLite<H: EventHandler> {
  inner: Rc<ConnectionLiteInner<H>>,
}

impl<H: EventHandler> ConnectionLite<H> {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions, event_handler: H) -> Self {
    ConnectionLite { inner: Rc::new(ConnectionLiteInner::new(endpoint, options, event_handler)) }
  }

  #[inline]
  pub fn start3(endpoint: String, options: ConnectionOptions, event_handler: H) -> Addr<Self> {
    ConnectionLite::start_in_arbiter(&ArbiterPool::singleton().fetch_arbiter(), move |_ctx| {
      ConnectionLite::new(endpoint, options, event_handler)
    })
  }

  pub fn start4(
    endpoint: String, options: ConnectionOptions, arbiter: ArbiterHandle, event_handler: H,
  ) -> Addr<Self> {
    ConnectionLite::start_in_arbiter(&arbiter, move |_ctx| {
      ConnectionLite::new(endpoint, options, event_handler)
    })
  }

  #[inline]
  pub async fn stop(addr: Addr<Self>) -> Result<(), SendError> {
    match addr.send(StopMsg).await {
      Ok(ok) => Ok(ok),
      Err(err) => match err {
        MailboxError::Closed => Err(SendError::Closed),
        MailboxError::Timeout => Err(SendError::Timeout),
      },
    }
  }
}

impl<H: EventHandler> Connection for ConnectionLite<H> {}

impl<H: EventHandler> Actor for ConnectionLite<H> {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Started: actor: {}<{}>", &self.inner.url, &self.inner.id);
    Box::pin(Rc::clone(&self.inner).connect_repeatedly().into_actor(self)).spawn(ctx);
    Box::pin(Rc::clone(&self.inner).ping_repeatedly_or_stop().into_actor(self)).spawn(ctx);
    Box::pin(Rc::clone(&self.inner).receive_repeatedly().into_actor(self)).spawn(ctx);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Stopping: actor: {}<{}>", &self.inner.url, &self.inner.id);
    self.inner.stop();
    Running::Stop
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    log::info!("Stopped: actor: {}<{}>", &self.inner.url, &self.inner.id);
  }
}

impl<H: EventHandler> Handler<ProtocolMsg> for ConnectionLite<H> {
  type Result = ResponseFuture<Result<ProtocolMsg, SendError>>;

  #[inline]
  fn handle(&mut self, msg: ProtocolMsg, _ctx: &mut Context<Self>) -> Self::Result {
    Box::pin(Rc::clone(&self.inner).send(msg))
  }
}

impl<H: EventHandler> Handler<NextMsgRefMsg> for ConnectionLite<H> {
  type Result = u32;

  fn handle(&mut self, _msg: NextMsgRefMsg, _ctx: &mut Context<Self>) -> Self::Result {
    self.inner.next_msg_ref()
  }
}

impl<H: EventHandler> Handler<StopMsg> for ConnectionLite<H> {
  type Result = ();

  fn handle(&mut self, _msg: StopMsg, ctx: &mut Context<Self>) -> Self::Result {
    log::info!("Received StopMsg: actor: {}<{}>", &self.inner.url, &self.inner.id);
    ctx.stop();
  }
}

impl<H: EventHandler> TimeoutExt for Request<ConnectionLite<H>, ProtocolMsg> {
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

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use std::time::Duration;

  use actix::prelude::*;
  use maxwell_protocol::IntoEnum;

  use crate::connection::connection_lite::{ConnectionLite, NextMsgRefMsg};
  use crate::connection::ConnectionOptions;
  use crate::connection::TimeoutExt;

  struct EventHandler;
  impl super::EventHandler for EventHandler {
    fn on_msg(&self, msg: maxwell_protocol::ProtocolMsg) {
      log::info!("Received msg: {:?}", msg);
    }
  }

  #[actix::test]
  async fn test_send_msg_with_connection_lite() {
    let conn = ConnectionLite::new(
      String::from("localhost:8081"),
      ConnectionOptions::default(),
      EventHandler,
    )
    .start();
    for _ in 1..2 {
      let next_msg_ref = conn.send(NextMsgRefMsg).await.unwrap();
      let msg = maxwell_protocol::PingReq { r#ref: next_msg_ref }.into_enum();
      let res = conn.send(msg).timeout_ext(Duration::from_millis(3000)).await;
      println!("with_connection_lite result: {:?}", res);
    }
  }
}
