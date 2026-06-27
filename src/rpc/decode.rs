//! Decoding msgpack-rpc responses from Neovim.
use std::{
    convert::TryInto,
    io::{self, ErrorKind, Read},
};

use bytes::{Buf, Bytes, BytesMut};
use rmp::{
    Marker,
    decode::{DecodeStringError, ValueReadError, bytes::BytesReadError},
};
use rmpv::{Value, decode::read_value};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    error::{DecodeError, InvalidMessage},
    rpc::skip::skip_value,
};

const DECODE_READ_BUFFER_SIZE: usize = 64 * 1024;
const MSG_TYPE_REQUEST: u64 = 0;
const MSG_TYPE_RESPONSE: u64 = 1;
const MSG_TYPE_NOTIFICATION: u64 = 2;

#[derive(Debug, PartialEq, Clone)]
pub struct RpcResponse {
    pub msgid: u64,
    pub error: Value,
    pub result: Value,
}

#[derive(Debug)]
pub enum IncomingMessage<'a> {
    Response(RpcResponse),
    Request { msgid: u64, method: &'a str },
    Notification { method: &'a str },
}

#[derive(Debug)]
pub struct DecodedMessage<'a> {
    pub message: IncomingMessage<'a>,
    pub consumed: usize,
}

/// State reused while decoding msgpack-rpc messages from a stream.
pub struct DecodeState {
    rest: BytesMut,
    start: usize,
    read_buf: Box<[u8; DECODE_READ_BUFFER_SIZE]>,
}

impl Default for DecodeState {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeState {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            rest: BytesMut::new(),
            start: 0,
            read_buf: Box::new([0; DECODE_READ_BUFFER_SIZE]),
        }
    }

    #[inline]
    pub fn has_rest(&self) -> bool {
        self.start < self.rest.len()
    }

    #[inline]
    pub fn rest(&self) -> &[u8] {
        &self.rest[self.start..]
    }

    pub fn try_decode_message(&self) -> Result<Option<DecodedMessage<'_>>, Box<DecodeError>> {
        let mut input = &self.rest[self.start..];
        let available_len = input.len();

        match IncomingMessage::decode(&mut input) {
            Ok(message) => Ok(Some(DecodedMessage {
                message,
                consumed: available_len - input.len(),
            })),
            Err(err) => {
                if let DecodeError::BufferError(e) = err.as_ref()
                    && e.kind() == ErrorKind::UnexpectedEof
                {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
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

    #[inline]
    pub fn take_rest(&mut self, consumed: usize) -> Bytes {
        self.compact_rest();
        self.rest.split_to(consumed).freeze()
    }

    pub async fn read_next_chunk<R>(&mut self, reader: &mut R) -> Result<(), Box<DecodeError>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        self.compact_rest();

        let n = reader.read(self.read_buf.as_mut()).await?;
        if n == 0 {
            Err(io::Error::new(ErrorKind::UnexpectedEof, "EOF").into())
        } else {
            self.rest.extend_from_slice(&self.read_buf[..n]);
            Ok(())
        }
    }

    #[inline]
    fn compact_rest(&mut self) {
        if self.start == 0 {
            return;
        }
        self.rest.advance(self.start);
        self.start = 0;
    }
}

struct EnvelopeReader<'a, R> {
    reader: &'a mut R,
    len: u32,
    read: u32,
}

impl<'a, R: Read> EnvelopeReader<'a, R> {
    #[inline]
    fn new(reader: &'a mut R) -> Result<Self, Box<DecodeError>> {
        Ok(Self {
            len: Self::read_len(reader)?,
            reader,
            read: 0,
        })
    }

    #[inline]
    fn len(&self) -> u32 {
        self.len
    }

    #[inline]
    fn require_len(&self, expected: u32) -> Result<(), Box<DecodeError>> {
        if self.len < expected {
            let expected = expected as u64;
            let err = InvalidMessage::WrongArrayLength(expected, expected, self.len as _);
            return Err(err.into());
        }
        Ok(())
    }

    #[inline]
    fn read_value(&mut self) -> Result<Value, Box<DecodeError>> {
        let value = read_value(self.reader)?;
        self.read += 1;
        Ok(value)
    }

    fn read_len(reader: &mut R) -> Result<u32, Box<DecodeError>> {
        match rmp::decode::read_array_len(reader) {
            Ok(len) => Ok(len),
            Err(ValueReadError::TypeMismatch(marker)) => {
                let value = read_value_from_marker(reader, marker)?;
                Err(InvalidMessage::NotAnArray(value).into())
            }
            Err(err) => Err(rmpv::decode::Error::from(err).into()),
        }
    }
}

impl<'reader, 'input> EnvelopeReader<'reader, &'input [u8]> {
    fn read_str(&mut self) -> Result<&'input str, Box<DecodeError>> {
        let (method, rest) =
            rmp::decode::read_str_from_slice(*self.reader).map_err(decode_string_error)?;
        *self.reader = rest;
        self.read += 1;
        Ok(method)
    }

    #[inline]
    fn finish(mut self) -> Result<(), Box<DecodeError>> {
        while self.read < self.len {
            let consumed = skip_value(self.reader)?;
            *self.reader = &self.reader[consumed..];
            self.read += 1;
        }
        Ok(())
    }
}

impl<'a> IncomingMessage<'a> {
    fn decode(reader: &mut &'a [u8]) -> Result<Self, Box<DecodeError>> {
        use crate::error::InvalidMessage::*;

        let mut fields = EnvelopeReader::new(reader)?;
        if fields.len() == 0 {
            return Err(WrongArrayLength(3, 4, fields.len() as _).into());
        }

        let msgtyp: u64 = fields.read_value()?.try_into().map_err(InvalidType)?;

        match msgtyp {
            MSG_TYPE_REQUEST => {
                fields.require_len(4)?;
                let msgid: u64 = fields.read_value()?.try_into().map_err(InvalidMsgid)?;
                let method = fields.read_str()?;
                fields.finish()?;
                Ok(Self::Request { msgid, method })
            }
            MSG_TYPE_NOTIFICATION => {
                fields.require_len(3)?;
                let method = fields.read_str()?;
                fields.finish()?;
                Ok(Self::Notification { method })
            }
            MSG_TYPE_RESPONSE => RpcResponse::decode(fields).map(Self::Response),
            t => Err(UnknownMessageType(t).into()),
        }
    }
}

impl RpcResponse {
    fn decode(mut fields: EnvelopeReader<'_, &[u8]>) -> Result<Self, Box<DecodeError>> {
        use crate::error::InvalidMessage::*;

        fields.require_len(4)?;

        let msgid: u64 = fields.read_value()?.try_into().map_err(InvalidMsgid)?;
        let error = fields.read_value()?;
        let result = fields.read_value()?;
        fields.finish()?;

        Ok(Self {
            msgid,
            error,
            result,
        })
    }
}

fn decode_string_error(err: DecodeStringError<'_, BytesReadError>) -> Box<DecodeError> {
    fn bytes_read_error(err: BytesReadError) -> io::Error {
        match err {
            BytesReadError::InsufficientBytes { .. } => {
                io::Error::new(ErrorKind::UnexpectedEof, err)
            }
            _ => io::Error::new(ErrorKind::InvalidData, err),
        }
    }

    let err = match err {
        DecodeStringError::InvalidMarkerRead(err) => {
            rmpv::decode::Error::InvalidMarkerRead(bytes_read_error(err))
        }
        DecodeStringError::InvalidDataRead(err) => {
            rmpv::decode::Error::InvalidDataRead(bytes_read_error(err))
        }
        DecodeStringError::BufferSizeTooSmall(_) => {
            let err = io::Error::new(ErrorKind::UnexpectedEof, "incomplete msgpack string");
            rmpv::decode::Error::InvalidDataRead(err)
        }
        DecodeStringError::TypeMismatch(_) => {
            let err = io::Error::new(ErrorKind::InvalidData, "expected msgpack string");
            rmpv::decode::Error::InvalidDataRead(err)
        }
        DecodeStringError::InvalidUtf8(_, _) => {
            let err = io::Error::new(ErrorKind::InvalidData, "invalid utf-8 msgpack string");
            rmpv::decode::Error::InvalidDataRead(err)
        }
    };
    err.into()
}

#[inline]
fn read_value_from_marker<R: Read>(
    reader: &mut R,
    marker: Marker,
) -> Result<Value, Box<DecodeError>> {
    let marker = [marker.to_u8()];
    let mut value_reader = Read::chain(io::Cursor::new(marker), reader);
    read_value(&mut value_reader).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::InvalidMessage;
    use crate::rpc::{
        encode::{RpcMessage, encode_sync},
        handler::{Dummy, Handler},
        redraw::{RedrawFrame, RedrawNotification},
    };
    use rmpv::encode::write_value;
    use std::{
        io::Cursor,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct CountingHandler;

    static UNKNOWN_REQUEST_COUNT: AtomicUsize = AtomicUsize::new(0);
    static UNKNOWN_REQUEST_MSGID: AtomicU64 = AtomicU64::new(0);

    impl Handler for CountingHandler {
        type Writer = Vec<u8>;

        fn handle_unknown_request(msgid: u64, name: &str) {
            assert_eq!(name, "ignored");
            UNKNOWN_REQUEST_MSGID.store(msgid, Ordering::Relaxed);
            UNKNOWN_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn request(msgid: u64, method: &str) -> RpcMessage {
        RpcMessage::RpcRequest {
            msgid,
            method: method.to_owned(),
            params: vec![],
        }
    }

    fn response(msgid: u64, result: Value) -> RpcMessage {
        RpcMessage::RpcResponse {
            msgid,
            error: Value::Nil,
            result,
        }
    }

    fn rpc_response(msgid: u64, result: Value) -> RpcResponse {
        RpcResponse {
            msgid,
            error: Value::Nil,
            result,
        }
    }

    fn notification(method: &str) -> RpcMessage {
        RpcMessage::RpcNotification {
            method: method.to_owned(),
            params: Vec::new(),
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

    fn decode_state_from_bytes(rest: Vec<u8>) -> DecodeState {
        DecodeState {
            rest: BytesMut::from(&rest[..]),
            start: 0,
            read_buf: Box::new([0; DECODE_READ_BUFFER_SIZE]),
        }
    }

    fn consume_next_response_from_state<H>(
        decoder: &mut DecodeState,
    ) -> Result<Option<RpcResponse>, Box<DecodeError>>
    where
        H: Handler,
    {
        loop {
            let (consumed, response) = {
                let Some(decoded) = decoder.try_decode_message()? else {
                    return Ok(None);
                };
                let consumed = decoded.consumed;

                let response = match decoded.message {
                    IncomingMessage::Response(response) => Some(response),
                    IncomingMessage::Request { msgid, method } => {
                        H::handle_unknown_request(msgid, method);
                        None
                    }
                    IncomingMessage::Notification { method } => {
                        H::handle_unknown_notify(method);
                        None
                    }
                };

                (consumed, response)
            };

            decoder.consume(consumed);

            if let Some(response) = response {
                return Ok(Some(response));
            }
            if !decoder.has_rest() {
                return Ok(None);
            }
        }
    }

    async fn decode_next_response_from_state(
        decoder: &mut DecodeState,
        reader: &mut Cursor<Vec<u8>>,
    ) -> RpcResponse {
        loop {
            while decoder.has_rest() {
                if let Some(response) =
                    consume_next_response_from_state::<Dummy<Vec<u8>>>(decoder).unwrap()
                {
                    return response;
                }
            }

            decoder.read_next_chunk(reader).await.unwrap();
        }
    }

    #[tokio::test]
    async fn decode_state_decodes_concatenated_responses() {
        let msg_1 = response(1, Value::from("one"));
        let msg_2 = response(2, Value::from("two"));
        let response_1 = rpc_response(1, Value::from("one"));
        let response_2 = rpc_response(2, Value::from("two"));

        let mut bytes = encoded(msg_1.clone());
        bytes.extend_from_slice(&encoded(msg_2.clone()));

        let mut reader = Cursor::new(bytes);
        let mut decoder = DecodeState::new();

        assert_eq!(
            decode_next_response_from_state(&mut decoder, &mut reader).await,
            response_1
        );
        assert_eq!(
            decode_next_response_from_state(&mut decoder, &mut reader).await,
            response_2
        );
    }

    #[tokio::test]
    async fn decode_state_ignores_requests_and_notifications_until_response() {
        let expected = rpc_response(3, Value::from("done"));
        let mut bytes = encoded(request(1, "ignored"));
        bytes.extend_from_slice(&encoded(notification("ignored")));
        bytes.extend_from_slice(&encoded(response(3, Value::from("done"))));

        let mut reader = Cursor::new(bytes);
        let mut decoder = DecodeState::new();

        assert_eq!(
            decode_next_response_from_state(&mut decoder, &mut reader).await,
            expected
        );
    }

    #[test]
    fn decode_state_calls_unknown_request_after_complete_payload() {
        UNKNOWN_REQUEST_COUNT.store(0, Ordering::Relaxed);
        UNKNOWN_REQUEST_MSGID.store(0, Ordering::Relaxed);

        let mut bytes = vec![
            Marker::FixArray(4).to_u8(),
            Marker::FixPos(0).to_u8(),
            Marker::FixPos(9).to_u8(),
            Marker::FixStr(7).to_u8(),
            b'i',
            b'g',
            b'n',
            b'o',
            b'r',
            b'e',
            b'd',
            Marker::FixArray(1).to_u8(),
            Marker::Str8.to_u8(),
            2,
            b'x',
        ];
        let mut decoder = decode_state_from_bytes(bytes.clone());

        assert!(
            consume_next_response_from_state::<CountingHandler>(&mut decoder)
                .unwrap()
                .is_none()
        );
        assert_eq!(UNKNOWN_REQUEST_COUNT.load(Ordering::Relaxed), 0);
        assert_eq!(decoder.rest(), bytes.as_slice());

        bytes.push(b'y');
        decoder.rest.extend_from_slice(&[b'y']);

        assert!(
            consume_next_response_from_state::<CountingHandler>(&mut decoder)
                .unwrap()
                .is_none()
        );
        assert_eq!(UNKNOWN_REQUEST_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(UNKNOWN_REQUEST_MSGID.load(Ordering::Relaxed), 9);
        assert!(!decoder.has_rest());
    }

    #[test]
    fn decode_state_rejects_short_request_without_consuming_next_frame() {
        let mut bytes = vec![
            Marker::FixArray(3).to_u8(),
            Marker::FixPos(0).to_u8(),
            Marker::FixPos(9).to_u8(),
            Marker::FixStr(7).to_u8(),
            b'i',
            b'g',
            b'n',
            b'o',
            b'r',
            b'e',
            b'd',
        ];
        bytes.extend_from_slice(&encoded(response(1, Value::from("ok"))));
        let decoder = decode_state_from_bytes(bytes.clone());

        let err = decoder.try_decode_message().unwrap_err();

        assert!(matches!(
            *err,
            DecodeError::InvalidMessage(InvalidMessage::WrongArrayLength(4, 4, 3))
        ));
        assert_eq!(decoder.rest(), bytes.as_slice());
    }

    #[test]
    fn decode_state_rejects_short_notification_without_consuming_next_frame() {
        let mut bytes = vec![
            Marker::FixArray(2).to_u8(),
            Marker::FixPos(2).to_u8(),
            Marker::FixStr(7).to_u8(),
            b'i',
            b'g',
            b'n',
            b'o',
            b'r',
            b'e',
            b'd',
        ];
        bytes.extend_from_slice(&encoded(response(1, Value::from("ok"))));
        let decoder = decode_state_from_bytes(bytes.clone());

        let err = decoder.try_decode_message().unwrap_err();

        assert!(matches!(
            *err,
            DecodeError::InvalidMessage(InvalidMessage::WrongArrayLength(3, 3, 2))
        ));
        assert_eq!(decoder.rest(), bytes.as_slice());
    }

    #[test]
    fn decode_state_skips_extra_response_fields_without_decoding_values() {
        let extra = Value::from(vec![
            Value::from(vec![(
                Value::from("nested"),
                Value::from(vec![Value::from(1), Value::Nil, Value::from("tail")]),
            )]),
            Value::from(true),
        ]);
        let bytes = encoded_value(Value::from(vec![
            Value::from(1),
            Value::from(9),
            Value::Nil,
            Value::from("ok"),
            extra,
        ]));
        let mut decoder = decode_state_from_bytes(bytes);

        let response = consume_next_response_from_state::<Dummy<Vec<u8>>>(&mut decoder)
            .unwrap()
            .unwrap();

        assert_eq!(response, rpc_response(9, Value::from("ok")));
        assert!(!decoder.has_rest());
    }

    #[test]
    fn decode_state_waits_when_extra_response_field_is_incomplete() {
        let mut bytes = vec![
            Marker::FixArray(5).to_u8(),
            Marker::FixPos(1).to_u8(),
            Marker::FixPos(9).to_u8(),
            Marker::Null.to_u8(),
            Marker::FixStr(2).to_u8(),
            b'o',
            b'k',
            Marker::Str8.to_u8(),
            2,
            b'x',
        ];
        let mut decoder = decode_state_from_bytes(bytes.clone());

        assert!(
            consume_next_response_from_state::<Dummy<Vec<u8>>>(&mut decoder)
                .unwrap()
                .is_none()
        );
        assert_eq!(decoder.rest(), bytes.as_slice());

        bytes.push(b'y');
        decoder.rest.extend_from_slice(&[b'y']);
        let response = consume_next_response_from_state::<Dummy<Vec<u8>>>(&mut decoder)
            .unwrap()
            .unwrap();

        assert_eq!(response, rpc_response(9, Value::from("ok")));
        assert!(!decoder.has_rest());
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
        let mut decoder = decode_state_from_bytes(rest);

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
        let mut decoder = decode_state_from_bytes(rest);

        decoder.consume(msg_1.len());
        let bytes = decoder.take_rest(msg_2.len());

        assert_eq!(&bytes[..], msg_2.as_slice());
        assert!(!decoder.has_rest());
    }
}
