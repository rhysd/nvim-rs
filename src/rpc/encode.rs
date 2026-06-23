//! Encoding msgpack-rpc messages to Neovim.
use std::io::Write;

use rmp::encode::{write_array_len, write_str, write_uint};
use rmpv::{
    Value, ValueRef,
    encode::{write_value, write_value_ref},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::EncodeError;

const MSG_TYPE_REQUEST: u64 = 0;
const MSG_TYPE_RESPONSE: u64 = 1;
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

#[inline]
fn write_value_array<W: Write>(writer: &mut W, values: &[Value]) -> Result<(), Box<EncodeError>> {
    write_array_len(writer, values.len() as u32)?;
    for value in values {
        write_value(writer, value)?;
    }
    Ok(())
}

/// Encode the given message into the `writer`.
pub fn encode_sync<W: Write>(writer: &mut W, msg: RpcMessage) -> Result<(), Box<EncodeError>> {
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
            write_uint(writer, MSG_TYPE_RESPONSE)?;
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

/// Encode a request or notification with one string argument without building
/// an owned [`RpcMessage`].
#[inline]
fn write_single_string_arg_msg<W: Write>(
    writer: &mut W,
    message_type: MessageType,
    method: &str,
    arg: &str,
) -> Result<(), Box<EncodeError>> {
    write_message_header(writer, message_type)?;
    write_str(writer, method)?;
    write_array_len(writer, 1)?;
    write_str(writer, arg)?;
    Ok(())
}

#[inline]
fn write_message_header<W: Write>(
    writer: &mut W,
    message_type: MessageType,
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
    Ok(())
}

#[inline]
fn write_message_value_ref<W: Write>(
    writer: &mut W,
    message_type: MessageType,
    method: &str,
    args: &[ValueRef<'_>],
) -> Result<(), Box<EncodeError>> {
    write_message_header(writer, message_type)?;
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
    #[inline]
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }

    #[inline]
    #[must_use]
    pub fn writer(&self) -> &W {
        &self.writer
    }

    #[inline]
    #[must_use]
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Encode the given message into the `BufWriter`. Flushes the writer when
/// finished.
pub async fn encode<W>(writer: &Mutex<W>, msg: RpcMessage) -> Result<(), Box<EncodeError>>
where
    W: AsyncWrite + Send + Unpin,
{
    let mut v: Vec<u8> = vec![];
    encode_sync(&mut v, msg)?;

    let mut writer = writer.lock().await;
    writer.write_all(&v).await?;
    writer.flush().await?;

    Ok(())
}

/// Encode the given message using a buffer reused with the writer.
pub async fn encode_to_state<W>(
    state: &Mutex<EncodeState<W>>,
    msg: RpcMessage,
) -> Result<(), Box<EncodeError>>
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut state = state.lock().await;
    let EncodeState { writer, buffer } = &mut *state;

    buffer.clear();
    encode_sync(buffer, msg)?;

    writer.write_all(buffer).await?;
    writer.flush().await?;

    Ok(())
}

/// Encode a request or notification with one string argument using a buffer
/// reused with the writer.
pub async fn encode_single_string_arg_msg_to_state<W: AsyncWrite + Send + Unpin + 'static>(
    state: &Mutex<EncodeState<W>>,
    message_type: MessageType,
    method: &str,
    arg: &str,
) -> Result<(), Box<EncodeError>> {
    let mut state = state.lock().await;
    let EncodeState { writer, buffer } = &mut *state;

    buffer.clear();
    write_single_string_arg_msg(buffer, message_type, method, arg)?;
    writer.write_all(buffer).await?;
    writer.flush().await?;

    Ok(())
}

/// Encode a request or notification using borrowed argument values.
pub async fn encode_value_ref_to_state<W>(
    state: &Mutex<EncodeState<W>>,
    message_type: MessageType,
    method: &str,
    args: &[ValueRef<'_>],
) -> Result<(), Box<EncodeError>>
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut state = state.lock().await;
    let EncodeState { writer, buffer } = &mut *state;

    buffer.clear();
    write_message_value_ref(buffer, message_type, method, args)?;
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
mod tests {
    use super::*;
    use rmpv::encode::write_value;

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
    fn encode_single_string_arg_sync_matches_rpc_message_encoding() {
        let mut request = Vec::new();
        write_single_string_arg_msg(&mut request, MessageType::Request(7), "nvim_input", "<C-D>")
            .unwrap();

        let via_request = encoded(RpcMessage::RpcRequest {
            msgid: 7,
            method: "nvim_input".to_owned(),
            params: vec![Value::from("<C-D>")],
        });

        assert_eq!(request, via_request);

        let mut notification = Vec::new();
        write_single_string_arg_msg(
            &mut notification,
            MessageType::Notification,
            "nvim_input",
            "<C-D>",
        )
        .unwrap();

        let via_notification = encoded(RpcMessage::RpcNotification {
            method: "nvim_input".to_owned(),
            params: vec![Value::from("<C-D>")],
        });

        assert_eq!(notification, via_notification);
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
        let opts = ValueRef::Map(vec![(ValueRef::from("output"), ValueRef::Boolean(true))]);

        let args = [cmd, opts];

        let mut direct = Vec::new();
        write_message_value_ref(&mut direct, MessageType::Request(7), "nvim_cmd", &args).unwrap();

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
}
