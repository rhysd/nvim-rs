//! Decoding and encoding msgpack rpc messages from/to neovim.
use std::{
  self,
  convert::TryInto,
  io::{self, ErrorKind, Read, Write},
  sync::Arc,
};

use futures::{
  io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
  lock::Mutex,
};
use rmpv::{decode::read_value, encode::write_value, Value};

use crate::error::{DecodeError, EncodeError};

const DECODE_READ_BUFFER_SIZE: usize = 80 * 1024;

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

macro_rules! rpc_args {
    ($($e:expr), *) => {{
        let vec = vec![
          $(Value::from($e),)*
        ];
        Value::from(vec)
    }}
}

/// State reused while decoding msgpack-rpc messages from a stream.
pub struct DecodeState {
  rest: Vec<u8>,
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
      rest: Vec::new(),
      read_buf: None,
    }
  }

  #[must_use]
  pub fn with_rest(rest: Vec<u8>) -> Self {
    Self {
      rest,
      read_buf: None,
    }
  }

  #[must_use]
  pub fn into_rest(self) -> Vec<u8> {
    self.rest
  }

  pub async fn decode<R>(
    &mut self,
    reader: &mut R,
  ) -> std::result::Result<RpcMessage, Box<DecodeError>>
  where
    R: AsyncRead + Send + Unpin + 'static,
  {
    loop {
      if let Some(msg) = try_decode_rest(&mut self.rest)? {
        return Ok(msg);
      }

      debug!("Not enough data, reading more!");
      let bytes_read = {
        let read_buf = self
          .read_buf
          .get_or_insert_with(|| Box::new([0; DECODE_READ_BUFFER_SIZE]));
        reader.read(&mut read_buf[..]).await
      };

      match bytes_read {
        Ok(n) if n == 0 => {
          return Err(io::Error::new(ErrorKind::UnexpectedEof, "EOF").into());
        }
        Ok(n) => {
          let read_buf = self
            .read_buf
            .as_ref()
            .expect("read buffer was initialized before reading");
          self.rest.extend_from_slice(&read_buf[..n]);
        }
        Err(err) => return Err(err.into()),
      }
    }
  }
}

/// Continously reads from reader, pushing onto `rest`. Then tries to decode the
/// contents of `rest`. If it succeeds, returns the message, and leaves any
/// non-decoded bytes in `rest`. If we did not read enough for a full message,
/// read more. Return on all other errors.
pub async fn decode<R: AsyncRead + Send + Unpin + 'static>(
  reader: &mut R,
  rest: &mut Vec<u8>,
) -> std::result::Result<RpcMessage, Box<DecodeError>> {
  let mut state = DecodeState::with_rest(std::mem::take(rest));
  let result = state.decode(reader).await;
  *rest = state.into_rest();
  result
}

fn try_decode_rest(
  rest: &mut Vec<u8>,
) -> std::result::Result<Option<RpcMessage>, Box<DecodeError>> {
  let original_len = rest.len();
  let mut input = rest.as_slice();

  match decode_buffer(&mut input).map_err(|b| *b) {
    Ok(msg) => {
      let consumed = original_len - input.len();
      discard_consumed(rest, consumed);
      Ok(Some(msg))
    }
    Err(DecodeError::BufferError(e))
      if e.kind() == ErrorKind::UnexpectedEof =>
    {
      Ok(None)
    }
    Err(err) => Err(err.into()),
  }
}

fn discard_consumed(rest: &mut Vec<u8>, consumed: usize) {
  if consumed == 0 {
    return;
  }

  if consumed >= rest.len() {
    rest.clear();
    return;
  }

  let remaining = rest.len() - consumed;
  rest.copy_within(consumed.., 0);
  rest.truncate(remaining);
}

/// Syncronously decode the content of a reader into an rpc message. Tries to
/// give detailed errors if something went wrong.
fn decode_buffer<R: Read>(
  reader: &mut R,
) -> std::result::Result<RpcMessage, Box<DecodeError>> {
  use crate::error::InvalidMessage::*;

  let arr: Vec<Value> = read_value(reader)?.try_into().map_err(NotAnArray)?;

  let mut arr = arr.into_iter();

  let msgtyp: u64 = arr
    .next()
    .ok_or(WrongArrayLength(3..=4, 0))?
    .try_into()
    .map_err(InvalidType)?;

  match msgtyp {
    0 => {
      let msgid: u64 = arr
        .next()
        .ok_or(WrongArrayLength(4..=4, 1))?
        .try_into()
        .map_err(InvalidMsgid)?;
      let method = match arr.next() {
        Some(Value::String(s)) if s.is_str() => {
          s.into_str().expect("Can remove using #230 of rmpv")
        }
        Some(val) => return Err(InvalidRequestName(msgid, val).into()),
        None => return Err(WrongArrayLength(4..=4, 2).into()),
      };
      let params: Vec<Value> = arr
        .next()
        .ok_or(WrongArrayLength(4..=4, 3))?
        .try_into()
        .map_err(|val| InvalidParams(val, method.clone()))?;

      Ok(RpcMessage::RpcRequest {
        msgid,
        method,
        params,
      })
    }
    1 => {
      let msgid: u64 = arr
        .next()
        .ok_or(WrongArrayLength(4..=4, 1))?
        .try_into()
        .map_err(InvalidMsgid)?;
      let error = arr.next().ok_or(WrongArrayLength(4..=4, 2))?;
      let result = arr.next().ok_or(WrongArrayLength(4..=4, 3))?;
      Ok(RpcMessage::RpcResponse {
        msgid,
        error,
        result,
      })
    }
    2 => {
      let method = match arr.next() {
        Some(Value::String(s)) if s.is_str() => {
          s.into_str().expect("Can remove using #230 of rmpv")
        }
        Some(val) => return Err(InvalidNotificationName(val).into()),
        None => return Err(WrongArrayLength(3..=3, 1).into()),
      };
      let params: Vec<Value> = arr
        .next()
        .ok_or(WrongArrayLength(3..=3, 2))?
        .try_into()
        .map_err(|val| InvalidParams(val, method.clone()))?;
      Ok(RpcMessage::RpcNotification { method, params })
    }
    t => Err(UnknownMessageType(t).into()),
  }
}

/// Encode the given message into the `writer`.
pub fn encode_sync<W: Write>(
  writer: &mut W,
  msg: RpcMessage,
) -> std::result::Result<(), Box<EncodeError>> {
  match msg {
    RpcMessage::RpcRequest {
      msgid,
      method,
      params,
    } => {
      let val = rpc_args!(0, msgid, method, params);
      write_value(writer, &val)?;
    }
    RpcMessage::RpcResponse {
      msgid,
      error,
      result,
    } => {
      let val = rpc_args!(1, msgid, error, result);
      write_value(writer, &val)?;
    }
    RpcMessage::RpcNotification { method, params } => {
      let val = rpc_args!(2, method, params);
      write_value(writer, &val)?;
    }
  };

  Ok(())
}

/// Encode the given message into the `BufWriter`. Flushes the writer when
/// finished.
pub async fn encode<W: AsyncWrite + Send + Unpin + 'static>(
  writer: Arc<Mutex<W>>,
  msg: RpcMessage,
) -> std::result::Result<(), Box<EncodeError>> {
  let mut v: Vec<u8> = vec![];
  encode_sync(&mut v, msg)?;

  let mut writer = writer.lock().await;
  writer.write_all(&v).await?;
  writer.flush().await?;

  Ok(())
}

pub trait IntoVal<T> {
  fn into_val(self) -> T;
}

impl<'a> IntoVal<Value> for &'a str {
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
      decode_buffer(&mut x.as_slice()).unwrap()
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
    let msg_dest_1 = decode_buffer(&mut cursor).unwrap();

    assert_eq!(msg_1, msg_dest_1);
    assert_eq!(16, cursor.position());

    let msg_dest_2 = decode_buffer(&mut cursor).unwrap();
    assert_eq!(msg_2, msg_dest_2);
  }
}
