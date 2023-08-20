use std::{
  cell::{Cell, RefCell},
  format,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use actix::{prelude::*, Addr};
use actix_codec::Framed;
use actix_web_actors::ws::{Frame, Message as WSMessage};
use awc::{ws::Codec, BoxedSocket, Client};
use futures_intrusive::sync::LocalManualResetEvent;
use futures_util::{
  sink::SinkExt,
  stream::{SplitSink, SplitStream, StreamExt},
};
use maxwell_protocol::{self, HandleError, ProtocolMsg, *};
use tokio::time::{sleep, timeout};

use super::*;
use crate::arbiter_pool::ArbiterPool;

static ID_SEED: AtomicU32 = AtomicU32::new(0);

pub trait EventHandler: Send + Sync + Unpin + Sized + 'static {
  #[inline(always)]
  fn on_msg(&self, _msg: ProtocolMsg) {}
  #[inline(always)]
  fn on_connected(&self, _addr: Addr<CallbackStyleConnection<Self>>) {}
  #[inline(always)]
  fn on_disconnected(&self, _addr: Addr<CallbackStyleConnection<Self>>) {}
  #[inline(always)]
  fn on_stopped(&self, _addr: Addr<CallbackStyleConnection<Self>>) {}
}

struct CallbackStyleConnectionInner<EH: EventHandler> {
  id: u32,
  addr: RefCell<Option<Addr<CallbackStyleConnection<EH>>>>,
  url: String,
  options: ConnectionOptions,
  sink: RefCell<Option<SplitSink<Framed<BoxedSocket, Codec>, WSMessage>>>,
  stream: RefCell<Option<SplitStream<Framed<BoxedSocket, Codec>>>>,
  connected_event: LocalManualResetEvent,
  disconnected_event: LocalManualResetEvent,
  is_connected: Cell<bool>,
  msg_ref: Cell<u32>,
  event_handler: EH,
  is_stopping: Cell<bool>,
}

impl<EH: EventHandler> CallbackStyleConnectionInner<EH> {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions, event_handler: EH) -> Self {
    CallbackStyleConnectionInner {
      id: ID_SEED.fetch_add(1, Ordering::Relaxed),
      addr: RefCell::new(None),
      url: Self::build_url(&endpoint),
      options,
      sink: RefCell::new(None),
      stream: RefCell::new(None),
      connected_event: LocalManualResetEvent::new(false),
      disconnected_event: LocalManualResetEvent::new(true),
      is_connected: Cell::new(false),
      msg_ref: Cell::new(1),
      event_handler,
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
      match Client::new()
        .ws(&self.url)
        .max_frame_size(self.options.max_frame_size as usize)
        .connect()
        .await
      {
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

  #[inline]
  pub async fn send(
    self: Rc<Self>, mut msg: ProtocolMsg,
  ) -> Result<ProtocolMsg, HandleError<ProtocolMsg>> {
    let mut msg_ref = maxwell_protocol::get_ref(&msg);
    if msg_ref == 0 {
      msg_ref = self.next_msg_ref();
      maxwell_protocol::set_ref(&mut msg, msg_ref);
    } else {
      self.try_set_msg_ref(msg_ref);
    }

    if !self.is_connected() {
      for i in 0..3 {
        if let Err(_) =
          timeout(Duration::from_millis(i * 500 + 500), self.connected_event.wait()).await
        {
          continue;
        } else {
          break;
        }
      }
      if !self.is_connected() {
        let desc = format!("Timeout to send msg: actor: {}<{}>", &self.url, &self.id);
        log::error!("{:?}", desc);
        return Err(HandleError::Any { code: 1, desc, msg });
      }
    }

    if let Err(err) =
      self.sink.borrow_mut().as_mut().unwrap().send(WSMessage::Binary(encode(&msg))).await
    {
      let desc = format!("Failed to send msg: actor: {}<{}>, err: {}", &self.url, &self.id, &err);
      log::error!("{:?}", desc);
      log::warn!(
        "The connection maybe broken, try to reconnect: actor: {}<{}>",
        &self.url,
        &self.id
      );
      self.toggle_to_disconnected();
      return Err(HandleError::Any { code: 2, desc, msg });
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
            Frame::Ping(_) => {}
            Frame::Pong(_) => {}
            Frame::Binary(bytes) => {
              let msg = maxwell_protocol::decode(&bytes).unwrap();
              self.event_handler.on_msg(msg);
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
    self.notify_stopped_event();
  }

  #[inline]
  fn notify_connected_event(&self) {
    let addr = self.addr.borrow();
    self.event_handler.on_connected(addr.as_ref().unwrap().clone());
  }

  #[inline]
  fn notify_disconnected_event(&self) {
    let addr = self.addr.borrow();
    self.event_handler.on_disconnected(addr.as_ref().unwrap().clone());
  }

  #[inline]
  fn notify_stopped_event(&self) {
    let addr = self.addr.borrow();
    self.event_handler.on_stopped(addr.as_ref().unwrap().clone());
  }

  #[inline]
  pub fn next_msg_ref(&self) -> u32 {
    let prev_msg_ref = self.msg_ref.get();
    if prev_msg_ref < MAX_MSG_REF {
      let curr_msg_ref = prev_msg_ref + 1;
      self.msg_ref.set(curr_msg_ref);
      curr_msg_ref
    } else {
      self.msg_ref.set(1);
      1
    }
  }

  #[inline]
  fn try_set_msg_ref(&self, msg_ref: u32) {
    if msg_ref > self.msg_ref.get() {
      self.msg_ref.set(msg_ref);
    }
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
    self.notify_connected_event();
  }

  #[inline]
  fn toggle_to_disconnected(&self) {
    self.is_connected.set(false);
    self.connected_event.reset();
    self.disconnected_event.set();
    self.notify_disconnected_event()
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

pub struct CallbackStyleConnection<EH: EventHandler> {
  inner: Rc<CallbackStyleConnectionInner<EH>>,
}
impl<EH: EventHandler> Connection for CallbackStyleConnection<EH> {}

impl<EH: EventHandler> Actor for CallbackStyleConnection<EH> {
  type Context = Context<Self>;

  #[inline]
  fn started(&mut self, ctx: &mut Self::Context) {
    log::info!("Started: actor: {}<{}>", &self.inner.url, &self.inner.id);
    *self.inner.addr.borrow_mut() = Some(ctx.address());

    ctx.set_mailbox_capacity(self.inner.options.mailbox_capacity as usize);

    Box::pin(Rc::clone(&self.inner).connect_repeatedly().into_actor(self)).spawn(ctx);
    Box::pin(Rc::clone(&self.inner).receive_repeatedly().into_actor(self)).spawn(ctx);
  }

  #[inline]
  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Stopping: actor: {}<{}>", &self.inner.url, &self.inner.id);
    self.inner.stop();
    Running::Stop
  }

  #[inline]
  fn stopped(&mut self, _: &mut Self::Context) {
    log::info!("Stopped: actor: {}<{}>", &self.inner.url, &self.inner.id);
  }
}

impl<EH: EventHandler> Handler<ProtocolMsg> for CallbackStyleConnection<EH> {
  type Result = ResponseFuture<Result<ProtocolMsg, HandleError<ProtocolMsg>>>;

  #[inline]
  fn handle(&mut self, msg: ProtocolMsg, _ctx: &mut Context<Self>) -> Self::Result {
    Box::pin(Rc::clone(&self.inner).send(msg))
  }
}

impl<EH: EventHandler> Handler<StopMsg> for CallbackStyleConnection<EH> {
  type Result = ();

  #[inline]
  fn handle(&mut self, _msg: StopMsg, ctx: &mut Context<Self>) -> Self::Result {
    log::info!("Received StopMsg: actor: {}<{}>", &self.inner.url, &self.inner.id);
    ctx.stop();
  }
}

impl<EH: EventHandler> Handler<DumpInfoMsg> for CallbackStyleConnection<EH> {
  type Result = ();

  #[inline]
  fn handle(&mut self, _msg: DumpInfoMsg, _ctx: &mut Context<Self>) -> Self::Result {
    log::info!("Connection info: id: {:?}, url: {:?}", self.inner.id, self.inner.url);
  }
}

impl<EH: EventHandler> CallbackStyleConnection<EH> {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions, event_handler: EH) -> Self {
    CallbackStyleConnection {
      inner: Rc::new(CallbackStyleConnectionInner::new(endpoint, options, event_handler)),
    }
  }

  #[inline]
  pub fn start3(endpoint: String, options: ConnectionOptions, event_handler: EH) -> Addr<Self> {
    CallbackStyleConnection::start_in_arbiter(
      &ArbiterPool::singleton().fetch_arbiter(),
      move |_ctx| CallbackStyleConnection::new(endpoint, options, event_handler),
    )
  }

  #[inline]
  pub fn start4(
    endpoint: String, options: ConnectionOptions, arbiter: ArbiterHandle, event_handler: EH,
  ) -> Addr<Self> {
    CallbackStyleConnection::start_in_arbiter(&arbiter, move |_ctx| {
      CallbackStyleConnection::new(endpoint, options, event_handler)
    })
  }

  #[inline]
  pub async fn stop(addr: Addr<Self>) -> Result<(), HandleError<StopMsg>> {
    match addr.send(StopMsg).await {
      Ok(ok) => Ok(ok),
      Err(err) => match err {
        MailboxError::Closed => Err(HandleError::MailboxClosed),
        MailboxError::Timeout => Err(HandleError::Timeout),
      },
    }
  }

  pub fn dump_info(addr: Addr<Self>) {
    addr.do_send(DumpInfoMsg);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use std::time::Duration;

  use actix::prelude::*;
  use futures_util::future::join_all;
  use maxwell_protocol::IntoEnum;

  use crate::connection::*;

  struct EventHandler;
  impl super::EventHandler for EventHandler {
    fn on_msg(&self, msg: maxwell_protocol::ProtocolMsg) {
      println!("Received msg: {:?}", msg);
    }
  }

  #[actix::test]
  async fn test_send_msg() {
    let conn = CallbackStyleConnection::<EventHandler>::new(
      String::from("localhost:8081"),
      ConnectionOptions::default(),
      EventHandler,
    )
    .start();
    for _ in 1..2 {
      let msg = maxwell_protocol::PingReq { r#ref: 0 }.into_enum();
      let res = conn.send(msg).timeout_ext(Duration::from_millis(3000)).await;
      println!("received result: {:?}", res);
    }
  }

  #[actix::test]
  async fn test_concurrent() {
    let conn = CallbackStyleConnection::<EventHandler>::new(
      String::from("localhost:8081"),
      ConnectionOptions::default(),
      EventHandler,
    )
    .start();

    // Spawn n threads.
    let threads: Vec<_> = (0..16_u8)
      .map(|thread_id| {
        let conn = conn.clone();
        tokio::spawn(async move {
          println!("Thread {} started.", thread_id);
          for _ in 0..10000 {
            let req = maxwell_protocol::PingReq { r#ref: 0 }.into_enum();
            let res = conn.send(req).timeout_ext(Duration::from_millis(3000)).await;
            println!("received result: res: {:?}, thread_id {:?}", res, thread_id);
          }
        })
      })
      .collect();

    join_all(threads).await;
  }
}
