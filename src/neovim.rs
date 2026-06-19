//! An active neovim session.
use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::{
  Mutex,
  mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
  oneshot,
};

use crate::{
  create::Spawner,
  error::{CallError, DecodeError, EncodeError, HandshakeError, LoopError},
  rpc::{
    handler::Handler,
    model,
    model::{IntoVal, MessageType, RpcMessage},
    redraw::{RedrawDecodeError, RedrawFrame, RedrawFrameInfo},
  },
  uioptions::UiAttachOptions,
};
use rmpv::{Value, ValueRef};

/// Pack the given arguments into a `Vec<Value>`, suitable for using it for a
/// [`call`](crate::neovim::Neovim::call) to neovim.
#[macro_export]
macro_rules! call_args {
    () => (Vec::new());
    ($($e:expr_2021),+,) => (call_args![$($e),*]);
    ($($e:expr_2021),+) => {{
        let vec = vec![
          $($e.into_val(),)*
        ];
        vec
    }};
}

type ResponseResult = Result<Result<Value, Value>, Arc<DecodeError>>;

type Queue = Mutex<Vec<(u64, oneshot::Sender<ResponseResult>)>>;

enum HandlerMessage {
  RpcMessage(RpcMessage),
  RedrawPayload(RedrawFrame),
}

impl std::fmt::Debug for HandlerMessage {
  fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::RpcMessage(msg) => {
        fmt.debug_tuple("RpcMessage").field(msg).finish()
      }
      Self::RedrawPayload(frame) => fmt
        .debug_struct("RedrawPayload")
        .field("len", &frame.consumed())
        .finish(),
    }
  }
}

/// An active Neovim session.
pub(crate) struct NeovimInner<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  writer: Mutex<model::EncodeState<W>>,
  queue: Queue,
  msgid_counter: AtomicU64,
}

pub struct Neovim<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  pub(crate) inner: Arc<NeovimInner<W>>,
}

impl<W> Clone for Neovim<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  fn clone(&self) -> Self {
    Neovim {
      inner: self.inner.clone(),
    }
  }
}

impl<W> PartialEq for Neovim<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  fn eq(&self, other: &Self) -> bool {
    Arc::ptr_eq(&self.inner, &other.inner)
  }
}
impl<W> Eq for Neovim<W> where W: AsyncWrite + Send + Unpin + 'static {}

impl<W> Neovim<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  #[allow(clippy::new_ret_no_self)]
  pub fn new<H, R>(
    reader: R,
    writer: W,
    handler: H,
  ) -> (
    Neovim<<H as Handler>::Writer>,
    impl Future<Output = Result<(), Box<LoopError>>>,
  )
  where
    R: AsyncRead + Send + Unpin + 'static,
    H: Handler<Writer = W> + Spawner,
  {
    let req = Neovim {
      inner: Arc::new(NeovimInner {
        writer: Mutex::new(model::EncodeState::new(writer)),
        queue: Mutex::new(Vec::new()),
        msgid_counter: AtomicU64::new(0),
      }),
    };

    let (sender, receiver) = unbounded_channel();
    let io_req = req.clone();
    let handler_req = req.clone();
    let fut = async move {
      tokio::try_join!(
        io_req.io_loop(reader, sender),
        handler_req.handler_loop(handler, receiver),
      )
      .map(|_| ())
    };

    (req, fut)
  }

  /// Create a new instance, immediately send a handshake message and
  /// wait for the response. Unlike `new`, this function is tolerant to extra
  /// data in the reader before the handshake response is received.
  ///
  /// `message` should be a unique string that is normally not found in the
  /// stdout. Due to the way Neovim packs strings, the length has to be either
  /// less than 20 characters or more than 31 characters long.
  /// See https://github.com/neovim/neovim/issues/32784 for more information.
  pub async fn handshake<H, R>(
    mut reader: R,
    writer: W,
    handler: H,
    message: &str,
  ) -> Result<
    (
      Neovim<<H as Handler>::Writer>,
      impl Future<Output = Result<(), Box<LoopError>>> + use<H, R, W>,
    ),
    Box<HandshakeError>,
  >
  where
    R: AsyncRead + Send + Unpin + 'static,
    H: Handler<Writer = W> + Spawner,
  {
    let instance = Neovim {
      inner: Arc::new(NeovimInner {
        writer: Mutex::new(model::EncodeState::new(writer)),
        queue: Mutex::new(Vec::new()),
        msgid_counter: AtomicU64::new(0),
      }),
    };

    let msgid = instance.inner.msgid_counter.fetch_add(1, Ordering::Relaxed);
    // Nvim encodes fixed size strings with a length of 20-31 bytes wrong, so
    // avoid that
    let msg_len = message.len();
    assert!(
      !(20..=31).contains(&msg_len),
      "The message should be less than 20 characters or more than 31 characters
      long, but the length is {msg_len}."
    );

    let req = RpcMessage::RpcRequest {
      msgid,
      method: "nvim_exec_lua".to_owned(),
      params: call_args![format!("return '{message}'"), Vec::<Value>::new()],
    };
    model::encode_to_state(&instance.inner.writer, req).await?;

    let expected_resp = RpcMessage::RpcResponse {
      msgid,
      error: rmpv::Value::Nil,
      result: rmpv::Value::String(message.into()),
    };
    let mut expected_data = Vec::new();
    model::encode_sync(&mut expected_data, expected_resp)
      .expect("Encoding static data can't fail");
    let mut actual_data = Vec::new();
    let mut start = 0;
    let mut end = 0;
    while end - start != expected_data.len() {
      actual_data.resize(start + expected_data.len(), 0);

      let bytes_read =
        reader
          .read(&mut actual_data[start..])
          .await
          .map_err(|err| {
            (
              err,
              String::from_utf8_lossy(&actual_data[..end]).to_string(),
            )
          })?;
      if bytes_read == 0 {
        // The end of the stream has been reached when the reader returns Ok(0).
        // Since we haven't detected a suitable response yet, return an error.
        return Err(Box::new(HandshakeError::UnexpectedResponse(
          String::from_utf8_lossy(&actual_data[..end]).to_string(),
        )));
      }
      end += bytes_read;
      while end - start > 0 {
        if actual_data[start..end] == expected_data[..end - start] {
          break;
        }
        start += 1;
      }
    }

    let (sender, receiver) = unbounded_channel();
    let io_instance = instance.clone();
    let handler_instance = instance.clone();
    let fut = async move {
      tokio::try_join!(
        io_instance.io_loop(reader, sender),
        handler_instance.handler_loop(handler, receiver),
      )
      .map(|_| ())
    };

    Ok((instance, fut))
  }

  async fn send_msg(
    &self,
    method: &str,
    args: Vec<Value>,
  ) -> Result<oneshot::Receiver<ResponseResult>, Box<EncodeError>> {
    let msgid = self.inner.msgid_counter.fetch_add(1, Ordering::Relaxed);

    let req = RpcMessage::RpcRequest {
      msgid,
      method: method.to_owned(),
      params: args,
    };

    let (sender, receiver) = oneshot::channel();

    self.inner.queue.lock().await.push((msgid, sender));

    model::encode_to_state(&self.inner.writer, req).await?;

    Ok(receiver)
  }

  async fn send_string(
    &self,
    method: &str,
    arg: &str,
  ) -> Result<oneshot::Receiver<ResponseResult>, Box<EncodeError>> {
    let msgid = self.inner.msgid_counter.fetch_add(1, Ordering::Relaxed);
    let (sender, receiver) = oneshot::channel();

    self.inner.queue.lock().await.push((msgid, sender));

    model::encode_single_string_arg_msg_to_state(
      &self.inner.writer,
      MessageType::Request(msgid),
      method,
      arg,
    )
    .await?;

    Ok(receiver)
  }

  async fn send_value_ref(
    &self,
    method: &str,
    args: &[ValueRef<'_>],
  ) -> Result<oneshot::Receiver<ResponseResult>, Box<EncodeError>> {
    let msgid = self.inner.msgid_counter.fetch_add(1, Ordering::Relaxed);
    let (sender, receiver) = oneshot::channel();

    self.inner.queue.lock().await.push((msgid, sender));

    model::encode_value_ref_to_state(
      &self.inner.writer,
      MessageType::Request(msgid),
      method,
      args,
    )
    .await?;

    Ok(receiver)
  }

  pub async fn call(
    &self,
    method: &str,
    args: Vec<Value>,
  ) -> Result<Result<Value, Value>, Box<CallError>> {
    let receiver = self
      .send_msg(method, args)
      .await
      .map_err(|e| CallError::SendError(*e, method.to_string()))?;

    receive_response(receiver, method).await
  }

  pub(crate) async fn call_nvim_input(
    &self,
    keys: &str,
  ) -> Result<Result<Value, Value>, Box<CallError>> {
    const METHOD: &str = "nvim_input";

    let receiver = self
      .send_string(METHOD, keys)
      .await
      .map_err(|e| CallError::SendError(*e, METHOD.to_owned()))?;

    receive_response(receiver, METHOD).await
  }

  pub(crate) async fn notify_string(
    &self,
    method: &str,
    arg: &str,
  ) -> Result<(), Box<CallError>> {
    model::encode_single_string_arg_msg_to_state(
      &self.inner.writer,
      MessageType::Notification,
      method,
      arg,
    )
    .await
    .map_err(|e| Box::new(CallError::SendError(*e, method.to_owned())))
  }

  pub async fn call_value_ref(
    &self,
    method: &str,
    args: &[ValueRef<'_>],
  ) -> Result<Result<Value, Value>, Box<CallError>> {
    let receiver = self
      .send_value_ref(method, args)
      .await
      .map_err(|e| CallError::SendError(*e, method.to_owned()))?;

    receive_response(receiver, method).await
  }

  pub async fn notify_value_ref(
    &self,
    method: &str,
    args: &[ValueRef<'_>],
  ) -> Result<(), Box<CallError>> {
    model::encode_value_ref_to_state(
      &self.inner.writer,
      MessageType::Notification,
      method,
      args,
    )
    .await
    .map_err(|e| Box::new(CallError::SendError(*e, method.to_owned())))
  }

  async fn send_error_to_callers(
    &self,
    queue: &Queue,
    err: DecodeError,
  ) -> Result<Arc<DecodeError>, Box<LoopError>> {
    let err = Arc::new(err);
    let mut v: Vec<u64> = vec![];

    let mut queue = queue.lock().await;
    queue.drain(0..).for_each(|sender| {
      let msgid = sender.0;
      sender
        .1
        .send(Err(err.clone()))
        .unwrap_or_else(|_| v.push(msgid));
    });

    if v.is_empty() {
      Ok(err)
    } else {
      Err((err, v).into())
    }
  }

  async fn handler_loop<H>(
    self,
    handler: H,
    mut receiver: UnboundedReceiver<HandlerMessage>,
  ) -> Result<(), Box<LoopError>>
  where
    H: Handler<Writer = W> + Spawner,
  {
    loop {
      let msg = match receiver.recv().await {
        Some(msg) => msg,
        /* If our receiver closes, that just means that io_handler started
         * shutting down. This is normal, so shut down along with it and don't
         * report an error
         */
        None => break Ok(()),
      };

      match msg {
        HandlerMessage::RedrawPayload(frame) => {
          let redraw = frame.notification()?;
          handler.handle_redraw(redraw)?;
        }
        HandlerMessage::RpcMessage(msg) => match msg {
          RpcMessage::RpcRequest {
            msgid,
            method,
            params,
          } => {
            let handler_c = handler.clone();
            let neovim = self.clone();
            let inner = self.inner.clone();

            handler.spawn(async move {
              let response =
                match handler_c.handle_request(method, params, neovim).await {
                  Ok(result) => RpcMessage::RpcResponse {
                    msgid,
                    result,
                    error: Value::Nil,
                  },
                  Err(error) => RpcMessage::RpcResponse {
                    msgid,
                    result: Value::Nil,
                    error,
                  },
                };

              let _ = model::encode_to_state(&inner.writer, response).await;
            });
          }
          RpcMessage::RpcNotification { method, params } => {
            let neovim = self.clone();
            handler.handle_notify(method, params, neovim).await;
          }
          RpcMessage::RpcResponse { .. } => unreachable!(),
        },
      }
    }
  }

  async fn io_loop<R>(
    self,
    mut reader: R,
    sender: UnboundedSender<HandlerMessage>,
  ) -> Result<(), Box<LoopError>>
  where
    R: AsyncRead + Send + Unpin + 'static,
  {
    let mut decoder = model::DecodeState::new();

    loop {
      let msg = match Self::decode_next(&mut decoder, &mut reader).await {
        Ok(msg) => msg,
        Err(err) => {
          let err = self.send_error_to_callers(&self.inner.queue, err).await?;
          return Err(Box::new(LoopError::DecodeError(err, None)));
        }
      };

      match msg {
        HandlerMessage::RpcMessage(RpcMessage::RpcResponse {
          msgid,
          result,
          error,
        }) => {
          let sender = find_sender(&self.inner.queue, msgid).await?;
          if error == Value::Nil {
            sender
              .send(Ok(Ok(result)))
              .map_err(|r| (msgid, r.expect("This was an OK(_)")))?;
          } else {
            sender
              .send(Ok(Err(error)))
              .map_err(|r| (msgid, r.expect("This was an OK(_)")))?;
          }
        }
        msg => {
          // Send message to handler_loop()
          sender.send(msg).unwrap();
        }
      }
    }
  }

  async fn decode_next<R>(
    decoder: &mut model::DecodeState,
    reader: &mut R,
  ) -> Result<HandlerMessage, DecodeError>
  where
    R: AsyncRead + Send + Unpin + 'static,
  {
    loop {
      if decoder.has_rest() {
        match RedrawFrameInfo::probe(decoder.rest()) {
          Ok(Some(info)) => {
            let bytes = decoder.take_rest(info.consumed());
            let frame = info.frame(bytes);
            return Ok(HandlerMessage::RedrawPayload(frame));
          }
          Ok(None) => {
            if let Some(msg) =
              decoder.try_decode_message().map_err(|err| *err)?
            {
              return Ok(HandlerMessage::RpcMessage(msg));
            }
          }
          Err(RedrawDecodeError::Incomplete) => {}
          Err(err) => return Err(err.into()),
        }
      }

      decoder.read_next_chunk(reader).await.map_err(|err| *err)?;
    }
  }

  /// Register as a remote UI.
  ///
  /// After this method is called, the client will receive redraw notifications.
  pub async fn ui_attach(
    &self,
    width: i64,
    height: i64,
    opts: &UiAttachOptions,
  ) -> Result<(), Box<CallError>> {
    let opts = opts.to_value_map();
    let args = [width.into(), height.into(), opts];

    self
      .call_value_ref("nvim_ui_attach", &args)
      .await?
      .map(|_| Ok(()))?
  }

  /// Send a quit command to Nvim.
  /// The quit command is 'qa!' which will make Nvim quit without
  /// saving anything.
  pub async fn quit_no_save(&self) -> Result<(), Box<CallError>> {
    self.command("qa!").await
  }
}

async fn receive_response(
  receiver: oneshot::Receiver<ResponseResult>,
  method: &str,
) -> Result<Result<Value, Value>, Box<CallError>> {
  match receiver.await {
    // Result<Result<Result<Value, Value>, Arc<DecodeError>>, RecvError>
    Ok(Ok(r)) => Ok(r), // r is Result<Value, Value>, i.e. we got an answer
    Ok(Err(err)) => {
      // err is a Decode Error, i.e. the answer wasn't decodable
      Err(Box::new(CallError::DecodeError(err, method.to_string())))
    }
    Err(err) => {
      // err is RecvError
      Err(Box::new(CallError::InternalReceiveError(
        err,
        method.to_string(),
      )))
    }
  }
}

/* The idea to use Vec here instead of HashMap
 * is that Vec is faster on small queue sizes
 * in most cases Vec.len = 1 so we just take first item in iteration.
 */
async fn find_sender(
  queue: &Queue,
  msgid: u64,
) -> Result<oneshot::Sender<ResponseResult>, Box<LoopError>> {
  let mut queue = queue.lock().await;

  let pos = match queue.iter().position(|req| req.0 == msgid) {
    Some(p) => p,
    None => return Err(msgid.into()),
  };
  let sender = queue.remove(pos).1;
  Ok(sender)
}

#[cfg(test)]
mod tests {
  use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
  };

  use rmpv::encode::write_value;
  use std::io::Cursor;

  use super::*;
  use crate::rpc::redraw::{RedrawDecodeResult, RedrawNotification};

  #[derive(Clone)]
  struct CountingHandler {
    redraw_count: Arc<AtomicUsize>,
  }

  impl Handler for CountingHandler {
    type Writer = Vec<u8>;

    fn handle_redraw(
      &self,
      mut redraw: RedrawNotification<'_>,
    ) -> RedrawDecodeResult<()> {
      redraw.for_each_batch(|batch| {
        assert_eq!(batch.name, "flush");
        assert!(batch.args.is_empty());
        Ok(true)
      })?;
      self.redraw_count.fetch_add(1, Ordering::Relaxed);
      Ok(())
    }
  }

  fn encoded_value(value: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_value(&mut bytes, &value).unwrap();
    bytes
  }

  fn redraw_bytes() -> Vec<u8> {
    encoded_value(Value::from(vec![
      Value::from(2),
      Value::from("redraw"),
      Value::from(vec![Value::from(vec![Value::from("flush")])]),
    ]))
  }

  fn test_neovim() -> Neovim<Vec<u8>> {
    Neovim {
      inner: Arc::new(NeovimInner {
        writer: Mutex::new(model::EncodeState::new(Vec::new())),
        queue: Mutex::new(Vec::new()),
        msgid_counter: AtomicU64::new(0),
      }),
    }
  }

  #[tokio::test]
  async fn decode_next_waits_for_complete_redraw_prefix() {
    let redraw = redraw_bytes();
    let prefix = [0x93, b'\x02', 0xa6, b'r', b'e', b'd', b'r', b'a', b'w'];

    assert!(redraw.starts_with(&prefix));

    let mut decoder = model::DecodeState::with_rest(prefix.to_vec());
    let mut reader = Cursor::new(redraw[prefix.len()..].to_vec());
    let msg = Neovim::<Vec<u8>>::decode_next(&mut decoder, &mut reader)
      .await
      .unwrap();

    match msg {
      HandlerMessage::RedrawPayload(frame) => {
        assert_eq!(frame.as_bytes(), redraw.as_slice());
      }
      msg => panic!("unexpected message: {msg:?}"),
    }
  }

  #[tokio::test]
  async fn handler_loop_handles_redraw_message() {
    let (sender, receiver) = unbounded_channel();
    let redraw_count = Arc::new(AtomicUsize::new(0));
    let handler = CountingHandler {
      redraw_count: redraw_count.clone(),
    };

    let frame = RedrawFrame::from_slice(&redraw_bytes()).unwrap();
    sender.send(HandlerMessage::RedrawPayload(frame)).unwrap();
    drop(sender);

    test_neovim().handler_loop(handler, receiver).await.unwrap();

    assert_eq!(redraw_count.load(Ordering::Relaxed), 1);
  }

  #[tokio::test]
  async fn notify_value_ref_writes_notification_without_queueing_request() {
    let nvim = test_neovim();

    nvim
      .notify_value_ref("nvim_ui_set_focus", &[ValueRef::Boolean(true)])
      .await
      .unwrap();

    assert!(nvim.inner.queue.lock().await.is_empty());

    let writer = nvim.inner.writer.lock().await;
    assert_eq!(
      writer.writer(),
      &encoded_value(Value::from(vec![
        Value::from(2),
        Value::from("nvim_ui_set_focus"),
        Value::from(vec![Value::from(true)]),
      ]))
    );
  }

  #[tokio::test]
  async fn test_find_sender() {
    let queue = Mutex::new(Vec::new());

    {
      let (sender, _receiver) = oneshot::channel();
      queue.lock().await.push((1, sender));
    }
    {
      let (sender, _receiver) = oneshot::channel();
      queue.lock().await.push((2, sender));
    }
    {
      let (sender, _receiver) = oneshot::channel();
      queue.lock().await.push((3, sender));
    }

    find_sender(&queue, 1).await.unwrap();
    assert_eq!(2, queue.lock().await.len());
    find_sender(&queue, 2).await.unwrap();
    assert_eq!(1, queue.lock().await.len());
    find_sender(&queue, 3).await.unwrap();
    assert!(queue.lock().await.is_empty());

    if let LoopError::MsgidNotFound(17) =
      *find_sender(&queue, 17).await.unwrap_err()
    {
    } else {
      panic!()
    }
  }
}
