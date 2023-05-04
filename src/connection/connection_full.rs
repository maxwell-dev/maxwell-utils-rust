use std::{
  cell::{Cell, RefCell},
  future::Future,
  pin::Pin,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
  task::{Context as TaskContext, Poll, Waker},
  time::Duration,
};

use actix::{prelude::*, Addr, Message as ActixMessage};
use actix_codec::Framed;
use actix_web_actors::ws::{Frame, Message as WSMessage};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use awc::{ws::Codec, BoxedSocket, Client};
use bytes::Bytes;
use futures_intrusive::sync::LocalManualResetEvent;
use futures_util::{
  sink::SinkExt,
  stream::{SplitSink, SplitStream, StreamExt},
};
use maxwell_protocol::{self, ProtocolMsg, SendError, *};
use tokio::time::sleep;

use super::connection::Options as ConnectionOptions;
use super::Connection;
use super::MAX_MSG_REF;
use crate::prelude::ArbiterPool;

static ID_SEED: AtomicU32 = AtomicU32::new(0);

struct Attachment {
  response: Option<ProtocolMsg>,
  waker: Option<Waker>,
}

struct Completer {
  msg_ref: u32,
  connection_inner: Rc<ConnectionFullInner>,
}

impl Completer {
  fn new(msg_ref: u32, connection_inner: Rc<ConnectionFullInner>) -> Self {
    connection_inner
      .attachments
      .borrow_mut()
      .insert(msg_ref, Attachment { response: None, waker: None });
    Completer { msg_ref, connection_inner }
  }
}

impl Drop for Completer {
  fn drop(&mut self) {
    self.connection_inner.attachments.borrow_mut().remove(&self.msg_ref);
  }
}

impl Future for Completer {
  type Output = ProtocolMsg;

  fn poll(self: Pin<&mut Self>, ctx: &mut TaskContext<'_>) -> Poll<ProtocolMsg> {
    let mut attachments = self.connection_inner.attachments.borrow_mut();
    let attachment = attachments.get_mut(&self.msg_ref).unwrap();
    if let Some(msg) = attachment.response.take() {
      Poll::Ready(msg)
    } else {
      match attachment.waker.as_ref() {
        None => {
          attachment.waker = Some(ctx.waker().clone());
        }
        Some(waker) => {
          if !waker.will_wake(ctx.waker()) {
            attachment.waker = Some(ctx.waker().clone());
          }
        }
      }
      Poll::Pending
    }
  }
}

struct ConnectionFullInner {
  id: u32,
  url: String,
  options: ConnectionOptions,
  sink: RefCell<Option<SplitSink<Framed<BoxedSocket, Codec>, WSMessage>>>,
  stream: RefCell<Option<SplitStream<Framed<BoxedSocket, Codec>>>>,
  connected_event: LocalManualResetEvent,
  disconnected_event: LocalManualResetEvent,
  is_connected: Cell<bool>,
  attachments: RefCell<HashMap<u32, Attachment>>,
  msg_ref: Cell<u32>,
  subscribers: RefCell<HashSet<Recipient<ConnectionStatusChangedMsg>>>,
  is_stopping: Cell<bool>,
}

impl ConnectionFullInner {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions) -> Self {
    ConnectionFullInner {
      id: ID_SEED.fetch_add(1, Ordering::Relaxed),
      url: Self::build_url(&endpoint),
      options,
      sink: RefCell::new(None),
      stream: RefCell::new(None),
      connected_event: LocalManualResetEvent::new(false),
      disconnected_event: LocalManualResetEvent::new(true),
      is_connected: Cell::new(false),
      attachments: RefCell::new(HashMap::new()),
      msg_ref: Cell::new(0),
      subscribers: RefCell::new(HashSet::new()),
      is_stopping: Cell::new(false),
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

      sleep(Duration::from_millis(self.options.ping_interval.unwrap() as u64)).await;
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

    let completer = Completer::new(msg_ref, Rc::clone(&self));

    if !self.is_connected() {
      self.connected_event.wait().await;
    }

    if let Err(err) =
      self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Binary(encode(&msg))).await
    {
      log::error!("Failed to send msg: err: {}", &err);
      return Err(SendError::Any(Box::new(err)));
    }

    Ok(completer.await)
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
              let response = maxwell_protocol::decode(&bytes).unwrap();
              let msg_ref = maxwell_protocol::get_ref(&response);
              let mut attachments = self.attachments.borrow_mut();
              if let Some(attachment) = attachments.get_mut(&msg_ref) {
                attachment.response = Some(response);
                attachment.waker.as_ref().unwrap().wake_by_ref();
              }
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
  pub fn subscribe(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.notify_connected(&r);
    self.subscribers.borrow_mut().insert(r);
  }

  #[inline]
  pub fn unsubscribe(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.subscribers.borrow_mut().remove(&r);
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
    self.notify_changed(ConnectionStatusChangedMsg::Connected);
  }

  #[inline]
  fn toggle_to_disconnected(&self) {
    self.is_connected.set(false);
    self.connected_event.reset();
    self.disconnected_event.set();
    self.notify_changed(ConnectionStatusChangedMsg::Disconnected);
  }

  #[inline]
  fn is_connected(&self) -> bool {
    self.is_connected.get()
  }

  #[inline]
  fn is_stopping(&self) -> bool {
    self.is_stopping.get()
  }

  #[inline]
  fn notify_changed(&self, status: ConnectionStatusChangedMsg) {
    let mut unavailables: Vec<Recipient<ConnectionStatusChangedMsg>> = Vec::new();
    for s in &*self.subscribers.borrow() {
      if s.connected() {
        s.do_send(status.clone());
      } else {
        unavailables.push(s.clone());
      }
    }
    for s in &unavailables {
      self.subscribers.borrow_mut().remove(s);
    }
  }

  #[inline]
  fn notify_connected(&self, r: &Recipient<ConnectionStatusChangedMsg>) {
    if self.is_connected() {
      r.do_send(ConnectionStatusChangedMsg::Connected);
    }
  }
}

pub struct ConnectionFull {
  inner: Rc<ConnectionFullInner>,
}

impl ConnectionFull {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions) -> Self {
    ConnectionFull { inner: Rc::new(ConnectionFullInner::new(endpoint, options)) }
  }

  #[inline]
  pub fn start2(endpoint: String, options: ConnectionOptions) -> Addr<Self> {
    ConnectionFull::start_in_arbiter(&ArbiterPool::singleton().fetch_arbiter(), move |_ctx| {
      ConnectionFull::new(endpoint, options)
    })
  }

  pub fn start3(
    endpoint: String, options: ConnectionOptions, arbiter: ArbiterHandle,
  ) -> Addr<Self> {
    ConnectionFull::start_in_arbiter(&arbiter, move |_ctx| ConnectionFull::new(endpoint, options))
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

impl Connection for ConnectionFull {}

impl Actor for ConnectionFull {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Started: actor: {}<{}>", &self.inner.url, &self.inner.id);
    Box::pin(Rc::clone(&self.inner).connect_repeatedly().into_actor(self)).spawn(ctx);
    if self.inner.options.ping_interval.is_some() {
      Box::pin(Rc::clone(&self.inner).ping_repeatedly_or_stop().into_actor(self)).spawn(ctx);
    }
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

impl Handler<ProtocolMsg> for ConnectionFull {
  type Result = ResponseFuture<Result<ProtocolMsg, SendError>>;

  #[inline]
  fn handle(&mut self, msg: ProtocolMsg, _ctx: &mut Context<Self>) -> Self::Result {
    Box::pin(Rc::clone(&self.inner).send(msg))
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "u32")]
pub struct NextMsgRefMsg;

impl Handler<NextMsgRefMsg> for ConnectionFull {
  type Result = u32;

  fn handle(&mut self, _msg: NextMsgRefMsg, _ctx: &mut Context<Self>) -> Self::Result {
    self.inner.next_msg_ref()
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct StopMsg;

impl Handler<StopMsg> for ConnectionFull {
  type Result = ();

  fn handle(&mut self, _msg: StopMsg, ctx: &mut Context<Self>) -> Self::Result {
    log::info!("Received StopMsg: actor: {}<{}>", &self.inner.url, &self.inner.id);
    ctx.stop();
  }
}

#[derive(Debug, ActixMessage, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub enum ConnectionStatusChangedMsg {
  Connected,
  Disconnected,
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct SubscribeConnectionStatusMsg(pub Recipient<ConnectionStatusChangedMsg>);

impl Handler<SubscribeConnectionStatusMsg> for ConnectionFull {
  type Result = ();

  fn handle(
    &mut self, msg: SubscribeConnectionStatusMsg, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.subscribe(msg.0);
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct UnsubscribeConnectionStatusMsg(pub Recipient<ConnectionStatusChangedMsg>);

impl Handler<UnsubscribeConnectionStatusMsg> for ConnectionFull {
  type Result = ();

  fn handle(
    &mut self, msg: UnsubscribeConnectionStatusMsg, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.unsubscribe(msg.0);
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

  use crate::connection::connection_full::{ConnectionFull, NextMsgRefMsg};
  use crate::connection::ConnectionOptions;
  use crate::connection::TimeoutExt;

  #[actix::test]
  async fn test_send_msg_with_connection_full() {
    let conn =
      ConnectionFull::new(String::from("localhost:8081"), ConnectionOptions::default()).start();
    for _ in 1..2 {
      let next_msg_ref = conn.send(NextMsgRefMsg).await.unwrap();
      let msg = maxwell_protocol::PingReq { r#ref: next_msg_ref }.into_enum();
      let res = conn.send(msg).timeout_ext(Duration::from_millis(1000)).await;
      println!("with_connection_full result: {:?}", res);
    }
  }
}
