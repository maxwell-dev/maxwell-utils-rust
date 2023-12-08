use std::{
  cell::{Cell, RefCell},
  fmt, format,
  future::Future,
  pin::Pin,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
  task::{Context as TaskContext, Poll, Waker},
  time::Duration,
};

use actix::{prelude::*, Message as ActixMessage};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::Error as AnyError;
use fastwebsockets::{
  handshake, FragmentCollectorRead, Frame, OpCode, WebSocketError, WebSocketWrite,
};
use futures_intrusive::sync::LocalManualResetEvent;
use hyper::{
  header::{CONNECTION, UPGRADE},
  upgrade::Upgraded,
  Body, Request as HyperRequest,
};
use maxwell_protocol::{self, HandleError, ProtocolMsg, *};
use tokio::{
  io::{split as tokio_split, ReadHalf, WriteHalf},
  net::TcpStream,
  task::spawn as tokio_spawn,
  time::{sleep, timeout},
};

use super::*;
use crate::arbiter_pool::ArbiterPool;

static ID_SEED: AtomicU32 = AtomicU32::new(0);

struct Attachment {
  response: Option<ProtocolMsg>,
  waker: Option<Waker>,
}

struct Completer {
  msg_ref: u32,
  connection_inner: Rc<FutureStyleConnectionInner>,
}

impl Completer {
  fn new(msg_ref: u32, connection_inner: Rc<FutureStyleConnectionInner>) -> Self {
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

// Tie hyper's executor to tokio runtime
struct SpawnExecutor;
impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
  Fut: Future + Send + 'static,
  Fut::Output: Send + 'static,
{
  fn execute(&self, fut: Fut) {
    tokio_spawn(fut);
  }
}

type Sink = WebSocketWrite<WriteHalf<Upgraded>>;
type Stream = FragmentCollectorRead<ReadHalf<Upgraded>>;

struct FutureStyleConnectionInner {
  id: u32,
  addr: RefCell<Option<Addr<FutureStyleConnection>>>,
  endpoint_index: Cell<usize>,
  endpoints: Vec<String>,
  options: ConnectionOptions,
  sink: RefCell<Option<Sink>>,
  stream: RefCell<Option<Stream>>,
  connected_event: LocalManualResetEvent,
  disconnected_event: LocalManualResetEvent,
  is_connected: Cell<bool>,
  attachments: RefCell<HashMap<u32, Attachment>>,
  msg_ref: Cell<u32>,
  observable_event_handlers: RefCell<HashMap<u64, Box<dyn ObservableEventHandler>>>,
  observable_event_actors: RefCell<HashSet<Recipient<ObservableEvent>>>,
  is_stopping: Cell<bool>,
}

impl FutureStyleConnectionInner {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions) -> Self {
    Self::with_alt_endpoints(vec![endpoint], options)
  }

  #[inline]
  pub fn with_alt_endpoints(endpoints: Vec<String>, options: ConnectionOptions) -> Self {
    FutureStyleConnectionInner {
      id: ID_SEED.fetch_add(1, Ordering::Relaxed),
      addr: RefCell::new(None),
      endpoint_index: Cell::new(endpoints.len() - 1),
      endpoints,
      options,
      sink: RefCell::new(None),
      stream: RefCell::new(None),
      connected_event: LocalManualResetEvent::new(false),
      disconnected_event: LocalManualResetEvent::new(true),
      is_connected: Cell::new(false),
      attachments: RefCell::new(HashMap::new()),
      msg_ref: Cell::new(1),
      observable_event_handlers: RefCell::new(HashMap::default()),
      observable_event_actors: RefCell::new(HashSet::new()),
      is_stopping: Cell::new(false),
    }
  }

  pub async fn connect_repeatedly(self: Rc<Self>) {
    loop {
      if self.is_stopping() {
        break;
      }

      self.disconnected_event.wait().await;
      self.close_sink().await.unwrap_or_else(|err| {
        log::error!(
          "Failed to close sink: actor: {}<{}>, err: {}",
          &self.curr_endpoint(),
          &self.id,
          err
        );
      });
      let endpoint = self.next_endpoint();
      log::info!("Connecting: actor: {}<{}>", &endpoint, &self.id);
      match self.connect(&endpoint).await {
        Ok((sink, stream)) => {
          log::info!("Connected: actor: {}<{}>", endpoint, &self.id);
          self.set_socket_pair(Some(sink), Some(stream));
          self.toggle_to_connected();
        }
        Err(err) => {
          log::error!("Failed to connect: actor: {}<{}>, err: {}", endpoint, &self.id, err);
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

    let completer = Completer::new(msg_ref, Rc::clone(&self));

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
        let desc = format!("Timeout to send msg: actor: {}<{}>", &self.curr_endpoint(), &self.id);
        log::error!("{:?}", desc);
        return Err(HandleError::Any { code: 1, desc, msg });
      }
    }

    if let Err(err) = self
      .sink
      .borrow_mut()
      .as_mut()
      .unwrap()
      .write_frame(Frame::binary(encode(&msg).as_ref().into()))
      .await
    {
      let curr_endpoint = self.curr_endpoint();
      let desc =
        format!("Failed to send msg: actor: {}<{}>, err: {}", &curr_endpoint, &self.id, &err);
      log::error!("{:?}", desc);
      log::warn!(
        "The connection maybe broken, try to reconnect: actor: {}<{}>",
        &curr_endpoint,
        &self.id
      );
      self.toggle_to_disconnected();
      return Err(HandleError::Any { code: 1, desc, msg });
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

      match self
        .stream
        .borrow_mut()
        .as_mut()
        .unwrap() // send_fn is empty because we does not create obligated writes here.
        .read_frame(&mut move |_| async { Ok::<_, WebSocketError>(()) })
        .await
      {
        Ok(frame) => match frame.opcode {
          OpCode::Ping => {}
          OpCode::Pong => {}
          OpCode::Binary => {
            let response = decode_bytes(&frame.payload).unwrap();
            let msg_ref = maxwell_protocol::get_ref(&response);
            let mut attachments = self.attachments.borrow_mut();
            if let Some(attachment) = attachments.get_mut(&msg_ref) {
              attachment.response = Some(response);
              attachment.waker.as_ref().unwrap().wake_by_ref();
            }
          }
          OpCode::Close => {
            log::error!(
              "Disconnected: actor: {}<{}>, reason: {:?}",
              &self.curr_endpoint(),
              &self.id,
              &frame.payload
            );
            self.toggle_to_disconnected();
          }
          other => {
            log::warn!("Received unknown msg: {:?}/{:?}", &other, &frame.payload);
          }
        },
        Err(err) => {
          log::error!("Protocol error occured: err: {}", &err);
          self.toggle_to_disconnected();
        }
      }
    }
  }

  #[inline]
  pub fn stop(&self) {
    self.is_stopping.set(true);
    self.notify_stopped_event();
  }

  #[inline]
  pub fn observe_observable_event_with_handler<OEH: ObservableEventHandler>(
    &self, msg: ObserveObservableEventWithHandlerMsg<OEH>,
  ) {
    if self.is_connected() {
      msg.handler.handle(ObservableEvent::Connected(self.addr.borrow().as_ref().unwrap().clone()));
    }
    self.observable_event_handlers.borrow_mut().insert(msg.handler.id(), Box::new(msg.handler));
  }

  #[inline]
  pub fn unobserve_observable_event_with_handler(
    &self, msg: UnobserveObservableEventWithHandlerMsg,
  ) {
    self.observable_event_handlers.borrow_mut().remove(&msg.handler_id);
  }

  #[inline]
  pub fn observe_observable_event_with_actor(&self, recip: Recipient<ObservableEvent>) {
    if self.is_connected() {
      recip.do_send(ObservableEvent::Connected(self.addr.borrow().as_ref().unwrap().clone()));
    }
    self.observable_event_actors.borrow_mut().insert(recip);
  }

  #[inline]
  pub fn unobserve_observable_event_with_actor(&self, recip: Recipient<ObservableEvent>) {
    self.observable_event_actors.borrow_mut().remove(&recip);
  }

  #[inline]
  fn notify_connected_event(&self) {
    self.notify_observable_event_for_handlers(ObservableEvent::Connected(
      self.addr.borrow().as_ref().unwrap().clone(),
    ));
    self.notify_observable_event_for_actors(ObservableEvent::Connected(
      self.addr.borrow().as_ref().unwrap().clone(),
    ))
  }

  #[inline]
  fn notify_disconnected_event(&self) {
    self.notify_observable_event_for_handlers(ObservableEvent::Disconnected(
      self.addr.borrow().as_ref().unwrap().clone(),
    ));
    self.notify_observable_event_for_actors(ObservableEvent::Disconnected(
      self.addr.borrow().as_ref().unwrap().clone(),
    ));
  }

  #[inline]
  fn notify_stopped_event(&self) {
    self.notify_observable_event_for_handlers(ObservableEvent::Stopped(
      self.addr.borrow().as_ref().unwrap().clone(),
    ));
    self.notify_observable_event_for_actors(ObservableEvent::Stopped(
      self.addr.borrow().as_ref().unwrap().clone(),
    ));
  }

  #[inline]
  fn notify_observable_event_for_handlers(&self, event: ObservableEvent) {
    let mut unavailables = Vec::new();
    let binding = self.observable_event_handlers.borrow();
    for (id, handler) in binding.iter() {
      if handler.is_available() {
        handler.handle(event.clone());
      } else {
        unavailables.push(id);
      }
    }
    for handler in &unavailables {
      self.observable_event_handlers.borrow_mut().remove(handler);
    }
  }

  #[inline]
  fn notify_observable_event_for_actors(&self, event: ObservableEvent) {
    let mut unavailables = Vec::new();
    for actor in &*self.observable_event_actors.borrow() {
      if actor.connected() {
        actor.do_send(event.clone());
      } else {
        unavailables.push(actor.clone());
      }
    }
    for actor in &unavailables {
      self.observable_event_actors.borrow_mut().remove(actor);
    }
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
  fn next_endpoint(&self) -> &String {
    let curr_endpoint_index = self.endpoint_index.get();
    let next_endpoint_index =
      if curr_endpoint_index >= self.endpoints.len() - 1 { 0 } else { curr_endpoint_index + 1 };
    self.endpoint_index.set(next_endpoint_index);
    &self.endpoints[next_endpoint_index]
  }

  #[inline]
  fn curr_endpoint(&self) -> &String {
    &self.endpoints[self.endpoint_index.get()]
  }

  #[inline]
  fn build_url(endpoint: &str) -> String {
    format!("ws://{}/$ws", endpoint)
  }

  async fn connect(&self, endpoint: &String) -> Result<(Sink, Stream), AnyError> {
    let stream = TcpStream::connect(endpoint).await?;
    let req = HyperRequest::builder()
      .method("GET")
      .uri(Self::build_url(endpoint))
      .header("Host", endpoint)
      .header(UPGRADE, "websocket")
      .header(CONNECTION, "upgrade")
      .header("CLIENT-ID", &format!("{}", self.id))
      .header("Sec-WebSocket-Key", handshake::generate_key())
      .header("Sec-WebSocket-Version", "13")
      .body(Body::empty())?;

    let (mut ws, _) = handshake::client(&SpawnExecutor, req, stream).await?;
    ws.set_auto_close(false);
    ws.set_auto_pong(false);
    ws.set_max_message_size(self.options.max_frame_size as usize);
    let (stream, sink) = ws.split(|s| tokio_split(s));
    Ok((sink, FragmentCollectorRead::new(stream)))
  }

  #[inline]
  fn set_socket_pair(&self, sink: Option<Sink>, stream: Option<Stream>) {
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
  async fn close_sink(&self) -> Result<(), AnyError> {
    if let Some(sink) = self.sink.try_borrow_mut()?.as_mut() {
      Ok(sink.write_frame(Frame::close_raw(vec![].into())).await?)
    } else {
      Ok(())
    }
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

pub struct FutureStyleConnection {
  inner: Rc<FutureStyleConnectionInner>,
}

impl Connection for FutureStyleConnection {}

impl Actor for FutureStyleConnection {
  type Context = Context<Self>;

  #[inline]
  fn started(&mut self, ctx: &mut Self::Context) {
    *self.inner.addr.borrow_mut() = Some(ctx.address());

    ctx.set_mailbox_capacity(self.inner.options.mailbox_capacity as usize);

    Box::pin(Rc::clone(&self.inner).connect_repeatedly().into_actor(self)).spawn(ctx);
    Box::pin(Rc::clone(&self.inner).receive_repeatedly().into_actor(self)).spawn(ctx);

    log::info!("Started: actor: {}<{}>", &self.inner.curr_endpoint(), &self.inner.id);
  }

  #[inline]
  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Stopping: actor: {}<{}>", &self.inner.curr_endpoint(), &self.inner.id);
    self.inner.stop();
    Running::Stop
  }

  #[inline]
  fn stopped(&mut self, _: &mut Self::Context) {
    log::info!("Stopped: actor: {}<{}>", &self.inner.curr_endpoint(), &self.inner.id);
  }
}

#[derive(Clone, ActixMessage, PartialEq, Eq)]
#[rtype(result = "()")]
pub enum ObservableEvent {
  Connected(Addr<FutureStyleConnection>),
  Disconnected(Addr<FutureStyleConnection>),
  Stopped(Addr<FutureStyleConnection>),
}

impl fmt::Debug for ObservableEvent {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Connected(_) => f.debug_tuple("Connected").finish(),
      Self::Disconnected(_) => f.debug_tuple("Disconnected").finish(),
      Self::Stopped(_) => f.debug_tuple("Closed").finish(),
    }
  }
}

pub trait ObservableEventHandler: Send + Sync + 'static {
  fn id(&self) -> u64;
  fn is_available(&self) -> bool;
  fn handle(&self, event: ObservableEvent);
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct ObserveObservableEventWithHandlerMsg<OEH: ObservableEventHandler> {
  pub handler: OEH,
}

impl<OEH: ObservableEventHandler> Handler<ObserveObservableEventWithHandlerMsg<OEH>>
  for FutureStyleConnection
{
  type Result = ();

  #[inline]
  fn handle(
    &mut self, msg: ObserveObservableEventWithHandlerMsg<OEH>, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.observe_observable_event_with_handler(msg);
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct UnobserveObservableEventWithHandlerMsg {
  pub handler_id: u64,
}

impl Handler<UnobserveObservableEventWithHandlerMsg> for FutureStyleConnection {
  type Result = ();

  #[inline]
  fn handle(
    &mut self, msg: UnobserveObservableEventWithHandlerMsg, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.unobserve_observable_event_with_handler(msg);
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct ObserveObservableEventWithActorMsg {
  pub recip: Recipient<ObservableEvent>,
}

impl Handler<ObserveObservableEventWithActorMsg> for FutureStyleConnection {
  type Result = ();

  fn handle(
    &mut self, msg: ObserveObservableEventWithActorMsg, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.observe_observable_event_with_actor(msg.recip);
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct UnobserveObservableEventWithActorMsg {
  pub recip: Recipient<ObservableEvent>,
}

impl Handler<UnobserveObservableEventWithActorMsg> for FutureStyleConnection {
  type Result = ();

  fn handle(
    &mut self, msg: UnobserveObservableEventWithActorMsg, _ctx: &mut Context<Self>,
  ) -> Self::Result {
    self.inner.unobserve_observable_event_with_actor(msg.recip);
  }
}

impl Handler<ProtocolMsg> for FutureStyleConnection {
  type Result = ResponseFuture<Result<ProtocolMsg, HandleError<ProtocolMsg>>>;

  #[inline]
  fn handle(&mut self, msg: ProtocolMsg, _ctx: &mut Context<Self>) -> Self::Result {
    Box::pin(Rc::clone(&self.inner).send(msg))
  }
}

impl Handler<StopMsg> for FutureStyleConnection {
  type Result = ();

  #[inline]
  fn handle(&mut self, _msg: StopMsg, ctx: &mut Context<Self>) -> Self::Result {
    ctx.stop();
  }
}

impl FutureStyleConnection {
  #[inline]
  pub fn new(endpoint: String, options: ConnectionOptions) -> Self {
    FutureStyleConnection { inner: Rc::new(FutureStyleConnectionInner::new(endpoint, options)) }
  }

  #[inline]
  pub fn with_alt_endpoints(endpoints: Vec<String>, options: ConnectionOptions) -> Self {
    FutureStyleConnection {
      inner: Rc::new(FutureStyleConnectionInner::with_alt_endpoints(endpoints, options)),
    }
  }

  #[inline]
  pub fn start2(endpoint: String, options: ConnectionOptions) -> Addr<Self> {
    FutureStyleConnection::start_in_arbiter(
      &ArbiterPool::singleton().fetch_arbiter(),
      move |_ctx| FutureStyleConnection::new(endpoint, options),
    )
  }

  #[inline]
  pub fn start_with_alt_endpoints2(
    endpoints: Vec<String>, options: ConnectionOptions,
  ) -> Addr<Self> {
    FutureStyleConnection::start_in_arbiter(
      &ArbiterPool::singleton().fetch_arbiter(),
      move |_ctx| FutureStyleConnection::with_alt_endpoints(endpoints, options),
    )
  }

  #[inline]
  pub fn start3(
    endpoint: String, options: ConnectionOptions, arbiter: ArbiterHandle,
  ) -> Addr<Self> {
    FutureStyleConnection::start_in_arbiter(&arbiter, move |_ctx| {
      FutureStyleConnection::new(endpoint, options)
    })
  }

  #[inline]
  pub fn start_with_alt_endpoints3(
    endpoints: Vec<String>, options: ConnectionOptions, arbiter: ArbiterHandle,
  ) -> Addr<Self> {
    FutureStyleConnection::start_in_arbiter(&arbiter, move |_ctx| {
      FutureStyleConnection::with_alt_endpoints(endpoints, options)
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
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use std::time::Duration;

  use actix::prelude::*;
  use maxwell_protocol::IntoEnum;

  use super::*;

  #[actix::test]
  async fn test_send_msg() {
    let conn =
      FutureStyleConnection::new(String::from("localhost:8081"), ConnectionOptions::default())
        .start();
    for _ in 1..2 {
      let msg = maxwell_protocol::PingReq { r#ref: 0 }.into_enum();
      let res = conn.send(msg).timeout_ext(Duration::from_millis(1000)).await;
      println!("with_connection_full result: {:?}", res);
    }
  }
}
