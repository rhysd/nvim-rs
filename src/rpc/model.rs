//! Decoding and encoding msgpack rpc messages from/to neovim.
use std::{
  self,
  convert::TryInto,
  io::{self, ErrorKind, Read, Write},
  sync::Arc,
};

use bytes::{Bytes, BytesMut};
use futures::{
  io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
  lock::Mutex,
};
use rmp::{
  Marker,
  decode::ValueReadError,
  encode::{write_array_len, write_str, write_uint},
};
use rmpv::{
  Value, ValueRef,
  decode::read_value,
  encode::{write_value, write_value_ref},
};

use crate::error::{DecodeError, EncodeError};

const DECODE_READ_BUFFER_SIZE: usize = 80 * 1024;
const MSG_TYPE_REQUEST: u64 = 0;
const MSG_TYPE_NOTIFICATION: u64 = 2;

pub enum MessageType {
  Request(u64),
  Notification,
}

/// A msgpack-rpc message, see
/// <https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md>
#[derive(Debug, PartialEq, Clone)]
pub enum RpcMessage {
  RpcRequest {
    msgid: u64,
    method: String,
    params: Vec<Value>,
  }, // 0
  RpcResponse {
    msgid: u64,
    error: Value,
    result: Value,
  }, // 1
  RpcNotification {
    method: String,
    params: Vec<Value>,
  }, // 2
}

/// State reused while decoding msgpack-rpc messages from a stream.
pub struct DecodeState {
  rest: BytesMut,
  start: usize,
  // OnceCell is not available because `get_mut_or_init` is not stabilized yet
  read_buf: Option<Box<[u8; DECODE_READ_BUFFER_SIZE]>>,
}

impl Default for DecodeState {
  fn default() -> Self {
    Self::new()
  }
}

impl DecodeState {
  #[must_use]
  pub fn new() -> Self {
    Self {
      rest: BytesMut::new(),
      start: 0,
      read_buf: None,
    }
  }

  #[must_use]
  pub fn with_rest(rest: Vec<u8>) -> Self {
    Self {
      rest: BytesMut::from(&rest[..]),
      start: 0,
      read_buf: None,
    }
  }

  pub fn has_rest(&self) -> bool {
    self.start < self.rest.len()
  }

  pub fn rest(&self) -> &[u8] {
    &self.rest[self.start..]
  }

  pub fn try_decode_message(
    &mut self,
  ) -> Result<Option<RpcMessage>, Box<DecodeError>> {
    match try_decode_slice(&self.rest[self.start..])? {
      Some((msg, consumed)) => {
        self.consume(consumed);
        Ok(Some(msg))
      }
      None => Ok(None),
    }
  }

  pub fn consume(&mut self, consumed: usize) {
    self.start += consumed;
    debug_assert!(self.start <= self.rest.len());
    if self.start == self.rest.len() {
      self.rest.clear();
      self.start = 0;
    }
  }

  pub fn take_rest(&mut self, consumed: usize) -> Bytes {
    self.compact_rest();
    self.rest.split_to(consumed).freeze()
  }

  pub async fn read_next_chunk<R>(
    &mut self,
    reader: &mut R,
  ) -> Result<(), Box<DecodeError>>
  where
    R: AsyncRead + Send + Unpin + 'static,
  {
    debug!("Not enough data, reading more!");
    self.compact_rest();

    let read_buf = self
      .read_buf
      .get_or_insert_with(|| Box::new([0; DECODE_READ_BUFFER_SIZE]))
      .as_mut();

    match reader.read(read_buf).await {
      Ok(0) => Err(io::Error::new(ErrorKind::UnexpectedEof, "EOF").into()),
      Ok(n) => {
        self.rest.extend_from_slice(&read_buf[..n]);
        Ok(())
      }
      Err(err) => Err(err.into()),
    }
  }

  fn compact_rest(&mut self) {
    if self.start == 0 {
      return;
    }

    let _ = self.rest.split_to(self.start);
    self.start = 0;
  }
}

fn try_decode_slice(
  bytes: &[u8],
) -> Result<Option<(RpcMessage, usize)>, Box<DecodeError>> {
  let available_len = bytes.len();
  let mut input = bytes;

  match RpcMessage::decode(&mut input).map_err(|b| *b) {
    Ok(msg) => Ok(Some((msg, available_len - input.len()))),
    Err(DecodeError::BufferError(e))
      if e.kind() == ErrorKind::UnexpectedEof =>
    {
      Ok(None)
    }
    Err(err) => Err(err.into()),
  }
}

struct EnvelopeReader<'a, R> {
  reader: &'a mut R,
  len: u64,
  read: u64,
}

impl<'a, R: Read> EnvelopeReader<'a, R> {
  #[inline]
  fn new(reader: &'a mut R) -> Result<Self, Box<DecodeError>> {
    let len = Self::read_len(reader)?;
    Ok(Self {
      reader,
      len,
      read: 0,
    })
  }

  #[inline]
  fn len(&self) -> u64 {
    self.len
  }

  #[inline]
  fn require_len(&self, expected: u64) -> Result<(), Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    if self.len < expected {
      return Err(WrongArrayLength(expected..=expected, self.len).into());
    }

    Ok(())
  }

  #[inline]
  fn read_value(&mut self) -> Result<Value, Box<DecodeError>> {
    let value = read_value(self.reader)?;
    self.read += 1;
    Ok(value)
  }

  #[inline]
  fn read_params(
    &mut self,
    method: &str,
  ) -> Result<Vec<Value>, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    match self.read_value_array()? {
      Ok(params) => Ok(params),
      Err(value) => Err(InvalidParams(value, method.to_owned()).into()),
    }
  }

  #[inline]
  fn finish(mut self) -> Result<(), Box<DecodeError>> {
    while self.read < self.len {
      read_value(self.reader)?;
      self.read += 1;
    }

    Ok(())
  }

  fn read_len(reader: &mut R) -> Result<u64, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    match rmp::decode::read_array_len(reader) {
      Ok(len) => Ok(u64::from(len)),
      Err(ValueReadError::TypeMismatch(marker)) => {
        Err(NotAnArray(read_value_from_marker(reader, marker)?).into())
      }
      Err(err) => Err(rmpv::decode::Error::from(err).into()),
    }
  }

  fn read_value_array(
    &mut self,
  ) -> Result<Result<Vec<Value>, Value>, Box<DecodeError>> {
    let mut len = match rmp::decode::read_array_len(self.reader) {
      Ok(len) => len,
      Err(ValueReadError::TypeMismatch(marker)) => {
        let value = read_value_from_marker(self.reader, marker)?;
        self.read += 1;
        return Ok(Err(value));
      }
      Err(err) => return Err(rmpv::decode::Error::from(err).into()),
    };

    let mut values = Vec::with_capacity(len as usize);
    while len > 0 {
      values.push(read_value(self.reader)?);
      len -= 1;
    }

    self.read += 1;
    Ok(Ok(values))
  }
}

impl RpcMessage {
  /// Syncronously decode the content of a reader into an rpc message. Tries to
  /// give detailed errors if something went wrong.
  fn decode<R: Read>(reader: &mut R) -> Result<Self, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    let mut fields = EnvelopeReader::new(reader)?;
    if fields.len() == 0 {
      return Err(WrongArrayLength(3..=4, fields.len()).into());
    }

    let msgtyp: u64 = fields.read_value()?.try_into().map_err(InvalidType)?;

    match msgtyp {
      0 => Self::decode_request(fields),
      1 => Self::decode_response(fields),
      2 => Self::decode_notification(fields),
      t => Err(UnknownMessageType(t).into()),
    }
  }

  fn decode_request<R: Read>(
    mut fields: EnvelopeReader<'_, R>,
  ) -> Result<Self, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    fields.require_len(4)?;

    let msgid: u64 = fields.read_value()?.try_into().map_err(InvalidMsgid)?;
    let method = match fields.read_value()? {
      Value::String(s) if s.is_str() => {
        s.into_str().expect("Can remove using #230 of rmpv")
      }
      val => return Err(InvalidRequestName(msgid, val).into()),
    };
    let params = fields.read_params(&method)?;
    fields.finish()?;

    Ok(Self::RpcRequest {
      msgid,
      method,
      params,
    })
  }

  fn decode_response<R: Read>(
    mut fields: EnvelopeReader<'_, R>,
  ) -> Result<Self, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    fields.require_len(4)?;

    let msgid: u64 = fields.read_value()?.try_into().map_err(InvalidMsgid)?;
    let error = fields.read_value()?;
    let result = fields.read_value()?;
    fields.finish()?;

    Ok(Self::RpcResponse {
      msgid,
      error,
      result,
    })
  }

  fn decode_notification<R: Read>(
    mut fields: EnvelopeReader<'_, R>,
  ) -> Result<Self, Box<DecodeError>> {
    use crate::error::InvalidMessage::*;

    fields.require_len(3)?;

    let method = match fields.read_value()? {
      Value::String(s) if s.is_str() => {
        s.into_str().expect("Can remove using #230 of rmpv")
      }
      val => return Err(InvalidNotificationName(val).into()),
    };
    let params = fields.read_params(&method)?;
    fields.finish()?;

    Ok(Self::RpcNotification { method, params })
  }
}

fn read_value_from_marker<R: Read>(
  reader: &mut R,
  marker: Marker,
) -> Result<Value, Box<DecodeError>> {
  let marker = [marker.to_u8()];
  let mut value_reader = io::Cursor::new(marker).chain(reader);
  read_value(&mut value_reader).map_err(Into::into)
}

fn write_value_array<W: Write>(
  writer: &mut W,
  values: &[Value],
) -> Result<(), Box<EncodeError>> {
  write_array_len(writer, values.len() as u32)?;
  for value in values {
    write_value(writer, value)?;
  }

  Ok(())
}

/// Encode the given message into the `writer`.
pub fn encode_sync<W: Write>(
  writer: &mut W,
  msg: RpcMessage,
) -> Result<(), Box<EncodeError>> {
  match msg {
    RpcMessage::RpcRequest {
      msgid,
      method,
      params,
    } => {
      write_array_len(writer, 4)?;
      write_uint(writer, MSG_TYPE_REQUEST)?;
      write_uint(writer, msgid)?;
      write_str(writer, &method)?;
      write_value_array(writer, &params)?;
    }
    RpcMessage::RpcResponse {
      msgid,
      error,
      result,
    } => {
      write_array_len(writer, 4)?;
      write_uint(writer, 1)?;
      write_uint(writer, msgid)?;
      write_value(writer, &error)?;
      write_value(writer, &result)?;
    }
    RpcMessage::RpcNotification { method, params } => {
      write_array_len(writer, 3)?;
      write_uint(writer, MSG_TYPE_NOTIFICATION)?;
      write_str(writer, &method)?;
      write_value_array(writer, &params)?;
    }
  };

  Ok(())
}

/// Encode an `nvim_input` request without building an owned [`RpcMessage`].
pub fn encode_nvim_input_sync<W: Write>(
  writer: &mut W,
  msgid: u64,
  keys: &str,
) -> Result<(), Box<EncodeError>> {
  write_array_len(writer, 4)?;
  write_uint(writer, MSG_TYPE_REQUEST)?;
  write_uint(writer, msgid)?;
  write_str(writer, "nvim_input")?;
  write_array_len(writer, 1)?;
  write_str(writer, keys)?;

  Ok(())
}

fn write_message_value_ref<W: Write>(
  writer: &mut W,
  message_type: MessageType,
  method: &str,
  args: &[ValueRef<'_>],
) -> Result<(), Box<EncodeError>> {
  match message_type {
    MessageType::Request(msgid) => {
      write_array_len(writer, 4)?;
      write_uint(writer, MSG_TYPE_REQUEST)?;
      write_uint(writer, msgid)?;
    }
    MessageType::Notification => {
      write_array_len(writer, 3)?;
      write_uint(writer, MSG_TYPE_NOTIFICATION)?;
    }
  }
  write_str(writer, method)?;
  write_array_len(writer, args.len() as u32)?;
  for arg in args {
    write_value_ref(writer, arg)?;
  }

  Ok(())
}

/// State reused while encoding msgpack-rpc messages to a stream.
pub struct EncodeState<W> {
  writer: W,
  buffer: Vec<u8>,
}

impl<W> EncodeState<W> {
  #[must_use]
  pub fn new(writer: W) -> Self {
    Self {
      writer,
      buffer: Vec::new(),
    }
  }

  #[must_use]
  pub fn into_inner(self) -> W {
    self.writer
  }

  #[must_use]
  pub fn get_ref(&self) -> &W {
    &self.writer
  }

  #[must_use]
  pub fn get_mut(&mut self) -> &mut W {
    &mut self.writer
  }
}

/// Encode the given message into the `BufWriter`. Flushes the writer when
/// finished.
pub async fn encode<W>(
  writer: Arc<Mutex<W>>,
  msg: RpcMessage,
) -> Result<(), Box<EncodeError>>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  let mut v: Vec<u8> = vec![];
  encode_sync(&mut v, msg)?;

  let mut writer = writer.lock().await;
  writer.write_all(&v).await?;
  writer.flush().await?;

  Ok(())
}

/// Encode the given message using a buffer reused with the writer.
pub async fn encode_with_state<W>(
  state: &Mutex<EncodeState<W>>,
  msg: RpcMessage,
) -> Result<(), Box<EncodeError>>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  let mut state = state.lock().await;
  state.buffer.clear();
  encode_sync(&mut state.buffer, msg)?;

  let EncodeState { writer, buffer } = &mut *state;
  writer.write_all(buffer).await?;
  writer.flush().await?;

  Ok(())
}

/// Encode an `nvim_input` request using a buffer reused with the writer.
pub async fn encode_nvim_input_with_state<
  W: AsyncWrite + Send + Unpin + 'static,
>(
  state: &Mutex<EncodeState<W>>,
  msgid: u64,
  keys: &str,
) -> Result<(), Box<EncodeError>> {
  let mut state = state.lock().await;
  state.buffer.clear();
  encode_nvim_input_sync(&mut state.buffer, msgid, keys)?;

  let EncodeState { writer, buffer } = &mut *state;
  writer.write_all(buffer).await?;
  writer.flush().await?;

  Ok(())
}

/// Encode a request or notification using borrowed argument values.
pub async fn encode_value_ref_with_state<W>(
  state: &Mutex<EncodeState<W>>,
  message_type: MessageType,
  method: &str,
  args: &[ValueRef<'_>],
) -> Result<(), Box<EncodeError>>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  let mut state = state.lock().await;
  state.buffer.clear();
  write_message_value_ref(&mut state.buffer, message_type, method, args)?;

  let EncodeState { writer, buffer } = &mut *state;
  writer.write_all(buffer).await?;
  writer.flush().await?;

  Ok(())
}

pub trait IntoVal<T> {
  fn into_val(self) -> T;
}

impl IntoVal<Value> for &str {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for Vec<String> {
  fn into_val(self) -> Value {
    let vec: Vec<Value> = self.into_iter().map(Value::from).collect();
    Value::from(vec)
  }
}

impl IntoVal<Value> for Vec<Value> {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for (i64, i64) {
  fn into_val(self) -> Value {
    Value::from(vec![Value::from(self.0), Value::from(self.1)])
  }
}

impl IntoVal<Value> for bool {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for i64 {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for f64 {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for String {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

impl IntoVal<Value> for Value {
  fn into_val(self) -> Value {
    self
  }
}

impl IntoVal<Value> for Vec<(Value, Value)> {
  fn into_val(self) -> Value {
    Value::from(self)
  }
}

#[cfg(test)]
mod decode_state_tests {
  use super::*;
  use crate::rpc::redraw::{RedrawFrame, RedrawNotification};
  use futures::{executor::block_on, io::Cursor};

  fn request(msgid: u64, method: &str) -> RpcMessage {
    RpcMessage::RpcRequest {
      msgid,
      method: method.to_owned(),
      params: vec![],
    }
  }

  fn encoded(msg: RpcMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_sync(&mut bytes, msg).unwrap();
    bytes
  }

  fn encoded_value(value: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_value(&mut bytes, &value).unwrap();
    bytes
  }

  fn redraw_frame(bytes: &[u8]) -> RedrawFrame {
    RedrawFrame::from_slice(bytes).unwrap()
  }

  fn decode_next_from_state(
    decoder: &mut DecodeState,
    reader: &mut Cursor<Vec<u8>>,
  ) -> RpcMessage {
    block_on(async {
      loop {
        while decoder.has_rest() {
          if let Some(msg) = decoder.try_decode_message().unwrap() {
            return msg;
          }
        }

        decoder.read_next_chunk(reader).await.unwrap();
      }
    })
  }

  #[test]
  fn encode_sync_matches_outer_value_encoding() {
    let request = RpcMessage::RpcRequest {
      msgid: 7,
      method: "nvim_input".to_owned(),
      params: vec![Value::from("<C-D>")],
    };
    assert_eq!(
      encoded(request),
      encoded_value(Value::from(vec![
        Value::from(0),
        Value::from(7),
        Value::from("nvim_input"),
        Value::from(vec![Value::from("<C-D>")]),
      ]))
    );

    let response = RpcMessage::RpcResponse {
      msgid: 8,
      error: Value::Nil,
      result: Value::from(true),
    };
    assert_eq!(
      encoded(response),
      encoded_value(Value::from(vec![
        Value::from(1),
        Value::from(8),
        Value::Nil,
        Value::from(true),
      ]))
    );

    let notification = RpcMessage::RpcNotification {
      method: "redraw".to_owned(),
      params: vec![Value::from(vec![Value::from("flush")])],
    };
    assert_eq!(
      encoded(notification),
      encoded_value(Value::from(vec![
        Value::from(2),
        Value::from("redraw"),
        Value::from(vec![Value::from(vec![Value::from("flush")])]),
      ]))
    );
  }

  #[test]
  fn encode_nvim_input_sync_matches_rpc_message_encoding() {
    let mut direct = Vec::new();
    encode_nvim_input_sync(&mut direct, 7, "<C-D>").unwrap();

    let via_message = encoded(RpcMessage::RpcRequest {
      msgid: 7,
      method: "nvim_input".to_owned(),
      params: vec![Value::from("<C-D>")],
    });

    assert_eq!(direct, via_message);
  }

  #[test]
  fn encode_value_ref_sync_matches_request_encoding() {
    let cmd = ValueRef::Map(vec![
      (ValueRef::from("cmd"), ValueRef::from("echo")),
      (
        ValueRef::from("args"),
        ValueRef::Array(vec![ValueRef::from("hello")]),
      ),
    ]);
    let opts =
      ValueRef::Map(vec![(ValueRef::from("output"), ValueRef::Boolean(true))]);

    let args = [cmd, opts];

    let mut direct = Vec::new();
    write_message_value_ref(
      &mut direct,
      MessageType::Request(7),
      "nvim_cmd",
      &args,
    )
    .unwrap();

    let via_message = encoded(RpcMessage::RpcRequest {
      msgid: 7,
      method: "nvim_cmd".to_owned(),
      params: args.iter().map(ValueRef::to_owned).collect(),
    });

    assert_eq!(direct, via_message);
  }

  #[test]
  fn encode_value_ref_sync_matches_notification_encoding() {
    let args = [ValueRef::from(120), ValueRef::from(40)];

    let mut direct = Vec::new();
    write_message_value_ref(
      &mut direct,
      MessageType::Notification,
      "nvim_ui_try_resize",
      &args,
    )
    .unwrap();

    let via_message = encoded(RpcMessage::RpcNotification {
      method: "nvim_ui_try_resize".to_owned(),
      params: args.iter().map(ValueRef::to_owned).collect(),
    });

    assert_eq!(direct, via_message);
  }

  #[test]
  fn envelope_reader_reads_fields_and_skips_extras() {
    let mut bytes = encoded_value(Value::from(vec![
      Value::from(1),
      Value::from(vec![Value::from(2), Value::from(3)]),
      Value::from("extra"),
    ]));
    bytes.extend_from_slice(&encoded_value(Value::from("tail")));

    let mut input = bytes.as_slice();
    let mut fields = EnvelopeReader::new(&mut input).unwrap();

    assert_eq!(fields.len(), 3);
    assert_eq!(fields.read, 0);
    assert_eq!(fields.read_value().unwrap(), Value::from(1));
    assert_eq!(fields.read, 1);
    assert_eq!(
      fields.read_value_array().unwrap().unwrap(),
      vec![Value::from(2), Value::from(3)]
    );
    assert_eq!(fields.read, 2);

    fields.finish().unwrap();
    assert_eq!(read_value(&mut input).unwrap(), Value::from("tail"));
    assert!(input.is_empty());
  }

  #[test]
  fn envelope_reader_reports_non_array_params_as_value() {
    let bytes = encoded_value(Value::from(vec![
      Value::from(2),
      Value::from("not-array"),
      Value::from("extra"),
    ]));
    let mut input = bytes.as_slice();
    let mut fields = EnvelopeReader::new(&mut input).unwrap();

    assert_eq!(fields.read_value().unwrap(), Value::from(2));
    let err = fields.read_params("redraw").unwrap_err();
    match *err {
      DecodeError::InvalidMessage(
        crate::error::InvalidMessage::InvalidParams(value, method),
      ) => {
        assert_eq!(value, Value::from("not-array"));
        assert_eq!(method, "redraw");
      }
      err => panic!("unexpected error: {err:?}"),
    }
    assert_eq!(fields.read, 2);

    fields.finish().unwrap();
    assert!(input.is_empty());
  }

  #[test]
  fn rpc_message_decode_ignores_extra_outer_array_items() {
    let msg = Value::from(vec![
      Value::from(0),
      Value::from(1),
      Value::from("test_method"),
      Value::from(Vec::<Value>::new()),
      Value::from("extra"),
    ]);
    let mut bytes = Vec::new();
    write_value(&mut bytes, &msg).unwrap();

    assert_eq!(
      RpcMessage::decode(&mut bytes.as_slice()).unwrap(),
      request(1, "test_method")
    );
  }

  #[test]
  fn decode_state_decodes_concatenated_messages() {
    let msg_1 = request(1, "test_method");
    let msg_2 = request(2, "test_method_2");

    let mut bytes = encoded(msg_1.clone());
    bytes.extend_from_slice(&encoded(msg_2.clone()));

    let mut reader = Cursor::new(bytes);
    let mut decoder = DecodeState::new();

    assert_eq!(decode_next_from_state(&mut decoder, &mut reader), msg_1);
    assert_eq!(decode_next_from_state(&mut decoder, &mut reader), msg_2);
  }

  #[test]
  fn decode_state_reads_redraw_frame_without_owned_message() {
    let redraw = encoded_value(Value::from(vec![
      Value::from(2),
      Value::from("redraw"),
      Value::from(vec![Value::from(vec![Value::from("flush")])]),
    ]));
    let msg = request(1, "test_method");
    let msg_bytes = encoded(msg.clone());

    let mut rest = redraw;
    rest.extend_from_slice(&msg_bytes);
    let mut decoder = DecodeState::with_rest(rest);

    let consumed = {
      let frame = redraw_frame(decoder.rest());
      let consumed = frame.consumed();
      let mut redraw: RedrawNotification<'_> = frame.notification().unwrap();

      assert_eq!(redraw.batch_count(), 1);
      redraw
        .for_each_batch(|batch| {
          assert_eq!(batch.name, "flush");
          assert!(batch.args.is_empty());
          Ok(true)
        })
        .unwrap();

      consumed
    };

    decoder.consume(consumed);
    assert_eq!(decoder.rest(), msg_bytes.as_slice());
  }

  #[test]
  fn decode_state_take_rest_compacts_consumed_prefix() {
    let msg_1 = encoded(request(1, "test_method"));
    let msg_2 = encoded(request(2, "test_method_2"));

    let mut rest = msg_1.clone();
    rest.extend_from_slice(&msg_2);
    let mut decoder = DecodeState::with_rest(rest);

    decoder.consume(msg_1.len());
    let bytes = decoder.take_rest(msg_2.len());

    assert_eq!(&bytes[..], msg_2.as_slice());
    assert!(!decoder.has_rest());
  }
}

#[cfg(all(test, feature = "use_tokio"))]
mod test {
  use super::*;
  use futures::{io::BufWriter, lock::Mutex};
  use std::{io::Cursor, sync::Arc};

  use tokio;

  #[tokio::test]
  async fn request_test() {
    let msg = RpcMessage::RpcRequest {
      msgid: 1,
      method: "test_method".to_owned(),
      params: vec![],
    };

    let buff: Vec<u8> = vec![];
    let tmp = Arc::new(Mutex::new(BufWriter::new(buff)));
    let tmp2 = tmp.clone();
    let msg2 = msg.clone();

    encode(tmp2, msg2).await.unwrap();

    let msg_dest = {
      let v = &mut *tmp.lock().await;
      let x = v.get_mut();
      RpcMessage::decode(&mut x.as_slice()).unwrap()
    };

    assert_eq!(msg, msg_dest);
  }

  #[tokio::test]
  async fn request_test_twice() {
    let msg_1 = RpcMessage::RpcRequest {
      msgid: 1,
      method: "test_method".to_owned(),
      params: vec![],
    };

    let msg_2 = RpcMessage::RpcRequest {
      msgid: 2,
      method: "test_method_2".to_owned(),
      params: vec![],
    };

    let buff: Vec<u8> = vec![];
    let tmp = Arc::new(Mutex::new(BufWriter::new(buff)));
    let msg_1_c = msg_1.clone();
    let msg_2_c = msg_2.clone();

    let tmp_c = tmp.clone();
    encode(tmp_c, msg_1_c).await.unwrap();
    let tmp_c = tmp.clone();
    encode(tmp_c, msg_2_c).await.unwrap();
    let len = (*tmp).lock().await.get_ref().len();
    assert_eq!(34, len); // Note: msg2 is 2 longer than msg

    let v = &mut *tmp.lock().await;
    let x = v.get_mut();
    let mut cursor = Cursor::new(x.as_slice());
    let msg_dest_1 = RpcMessage::decode(&mut cursor).unwrap();

    assert_eq!(msg_1, msg_dest_1);
    assert_eq!(16, cursor.position());

    let msg_dest_2 = RpcMessage::decode(&mut cursor).unwrap();
    assert_eq!(msg_2, msg_dest_2);
  }

  #[tokio::test]
  async fn encode_with_state_reuses_buffer() {
    let msg_1 = RpcMessage::RpcRequest {
      msgid: 1,
      method: "test_method".to_owned(),
      params: vec![],
    };

    let msg_2 = RpcMessage::RpcRequest {
      msgid: 2,
      method: "test_method".to_owned(),
      params: vec![],
    };

    let buff: Vec<u8> = vec![];
    let state = Arc::new(Mutex::new(EncodeState::new(BufWriter::new(buff))));

    encode_with_state(&state, msg_1.clone()).await.unwrap();
    let first_capacity = state.lock().await.buffer.capacity();
    assert!(first_capacity > 0);

    encode_with_state(&state, msg_2.clone()).await.unwrap();
    let mut state = state.lock().await;
    assert_eq!(first_capacity, state.buffer.capacity());

    let x = state.writer.get_mut();
    let mut cursor = Cursor::new(x.as_slice());
    let msg_dest_1 = RpcMessage::decode(&mut cursor).unwrap();
    let msg_dest_2 = RpcMessage::decode(&mut cursor).unwrap();

    assert_eq!(msg_1, msg_dest_1);
    assert_eq!(msg_2, msg_dest_2);
  }
}
