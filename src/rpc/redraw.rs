//! Borrowed msgpack readers for a future redraw notification fast path.

use std::{
    fmt::Debug,
    io::{self, ErrorKind},
    sync::Arc,
};

use rmp::{
    Marker,
    decode::{
        self, Bytes, DecodeStringError, NumValueReadError, RmpRead, ValueReadError,
        bytes::BytesReadError,
    },
};
use rmpv::{ValueRef, decode::read_value_ref};

use crate::error::{DecodeError, LoopError};

use super::skip::skip_value;

pub type RedrawDecodeResult<T> = Result<T, RedrawDecodeError>;

#[derive(Debug)]
pub enum RedrawDecodeError {
    Incomplete,
    Invalid(String),
}

impl RedrawDecodeError {
    fn new(err: impl Debug) -> Self {
        Self::Invalid(format!("unexpected msgpack payload {err:?}"))
    }
}

impl From<RedrawDecodeError> for DecodeError {
    fn from(value: RedrawDecodeError) -> Self {
        match value {
            RedrawDecodeError::Incomplete => Self::ReaderError(io::Error::new(
                ErrorKind::UnexpectedEof,
                "incomplete msgpack payload",
            )),
            RedrawDecodeError::Invalid(message) => {
                Self::ReaderError(io::Error::new(ErrorKind::InvalidData, message))
            }
        }
    }
}

impl From<Box<DecodeError>> for RedrawDecodeError {
    fn from(err: Box<DecodeError>) -> Self {
        match *err {
            DecodeError::BufferError(err) => err.into(),
            DecodeError::ReaderError(err) => err.into(),
            err => Self::new(err),
        }
    }
}

impl From<RedrawDecodeError> for Box<LoopError> {
    fn from(value: RedrawDecodeError) -> Self {
        Box::new(LoopError::DecodeError(Arc::new(value.into())))
    }
}

impl From<io::Error> for RedrawDecodeError {
    #[inline]
    fn from(err: io::Error) -> Self {
        if err.kind() == ErrorKind::UnexpectedEof {
            Self::Incomplete
        } else {
            Self::new(err)
        }
    }
}

impl From<BytesReadError> for RedrawDecodeError {
    #[inline]
    fn from(err: BytesReadError) -> Self {
        match err {
            BytesReadError::InsufficientBytes { .. } => Self::Incomplete,
            err => Self::new(err),
        }
    }
}

impl From<decode::MarkerReadError<BytesReadError>> for RedrawDecodeError {
    #[inline]
    fn from(err: decode::MarkerReadError<BytesReadError>) -> Self {
        err.0.into()
    }
}

impl From<ValueReadError<BytesReadError>> for RedrawDecodeError {
    fn from(err: ValueReadError<BytesReadError>) -> Self {
        match err {
            ValueReadError::InvalidMarkerRead(err) | ValueReadError::InvalidDataRead(err) => {
                err.into()
            }
            err => Self::new(err),
        }
    }
}

impl From<ValueReadError<io::Error>> for RedrawDecodeError {
    fn from(err: ValueReadError<io::Error>) -> Self {
        match err {
            ValueReadError::InvalidMarkerRead(err) | ValueReadError::InvalidDataRead(err) => {
                err.into()
            }
            err => Self::new(err),
        }
    }
}

impl From<NumValueReadError<BytesReadError>> for RedrawDecodeError {
    fn from(err: NumValueReadError<BytesReadError>) -> Self {
        match err {
            NumValueReadError::InvalidMarkerRead(err) | NumValueReadError::InvalidDataRead(err) => {
                err.into()
            }
            err => Self::new(err),
        }
    }
}

impl From<DecodeStringError<'_, BytesReadError>> for RedrawDecodeError {
    #[inline]
    fn from(err: DecodeStringError<'_, BytesReadError>) -> Self {
        match err {
            DecodeStringError::InvalidMarkerRead(err) | DecodeStringError::InvalidDataRead(err) => {
                err.into()
            }
            DecodeStringError::BufferSizeTooSmall(_) => Self::Incomplete,
            err => Self::new(err),
        }
    }
}

impl From<rmpv::decode::Error> for RedrawDecodeError {
    #[inline]
    fn from(err: rmpv::decode::Error) -> Self {
        match err {
            rmpv::decode::Error::InvalidMarkerRead(err)
            | rmpv::decode::Error::InvalidDataRead(err) => err.into(),
            err => Self::new(err),
        }
    }
}

/// A complete owned `redraw` notification frame.
pub struct RedrawFrame {
    bytes: bytes::Bytes,
    params_offset: usize,
    params_len: u32,
}

pub struct RedrawFrameInfo {
    consumed: usize,
    params_offset: usize,
    params_len: u32,
}

impl RedrawFrameInfo {
    pub fn probe(bytes: &[u8]) -> RedrawDecodeResult<Option<Self>> {
        let mut reader = MsgpackReader::new(bytes);
        let outer_len = match reader.read_rmp(decode::read_array_len) {
            Ok(len) => len,
            Err(ValueReadError::TypeMismatch(_)) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        if outer_len < 3 {
            return Ok(None);
        }

        let msg_type = match reader.read_rmp(decode::read_int::<u64, _>) {
            Ok(msg_type) => msg_type,
            Err(NumValueReadError::TypeMismatch(_)) | Err(NumValueReadError::OutOfRange) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        if msg_type != 2 {
            return Ok(None);
        }

        match reader.read_str_eq("redraw") {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            Err(err) => return Err(err),
        }

        let params_len = match reader.read_rmp(decode::read_array_len) {
            Ok(len) => len,
            Err(ValueReadError::TypeMismatch(_)) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let params_offset = reader.position;
        reader.skip_values(params_len)?;

        for _ in 3..outer_len {
            reader.skip_value()?;
        }

        Ok(Some(Self {
            consumed: reader.position,
            params_offset,
            params_len,
        }))
    }

    #[inline]
    #[must_use]
    pub fn consumed(&self) -> usize {
        self.consumed
    }

    #[inline]
    pub fn frame(&self, bytes: bytes::Bytes) -> RedrawFrame {
        debug_assert_eq!(bytes.len(), self.consumed);
        RedrawFrame {
            bytes,
            params_offset: self.params_offset,
            params_len: self.params_len,
        }
    }
}

impl Debug for RedrawFrame {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("RedrawFrame")
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl RedrawFrame {
    pub fn from_slice(bytes: &[u8]) -> RedrawDecodeResult<Self> {
        let info = RedrawFrameInfo::probe(bytes)?.expect("redraw frame");
        Ok(info.frame(bytes::Bytes::copy_from_slice(&bytes[..info.consumed])))
    }

    pub fn from_bytes(bytes: bytes::Bytes) -> RedrawDecodeResult<Self> {
        let info = RedrawFrameInfo::probe(&bytes)?.expect("redraw frame");
        Ok(info.frame(bytes))
    }

    #[inline]
    pub fn consumed(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub fn notification(&self) -> RedrawDecodeResult<RedrawNotification<'_>> {
        Ok(RedrawNotification::new(ArrayReader {
            reader: MsgpackReader {
                input: &self.bytes,
                position: self.params_offset,
            },
            remaining: self.params_len,
        }))
    }
}

/// The params of a `redraw` notification.
pub struct RedrawNotification<'de> {
    params: ArrayReader<'de>,
}

impl<'de> RedrawNotification<'de> {
    #[inline]
    #[must_use]
    pub(crate) fn new(params: ArrayReader<'de>) -> Self {
        Self { params }
    }

    #[inline]
    #[must_use]
    pub fn batch_count(&self) -> u32 {
        self.params.remaining()
    }

    pub fn for_each_batch<F>(&mut self, mut f: F) -> RedrawDecodeResult<()>
    where
        F: FnMut(&mut RedrawBatch<'de>) -> RedrawDecodeResult<bool>,
    {
        while !self.params.is_empty() {
            self.params.ensure_remaining()?;

            let mut batch_items = self.params.reader.read_array_reader()?;
            self.params.remaining -= 1;

            let name = batch_items.read_str()?;
            let args = batch_items;
            let mut batch = RedrawBatch { name, args };

            let should_continue = f(&mut batch)?;
            batch.args.skip_remaining()?;
            self.params.reader.position = batch.args.reader.position;

            if !should_continue {
                break;
            }
        }

        Ok(())
    }
}

/// One redraw event batch, e.g. `["grid_line", [...], ...]`.
pub struct RedrawBatch<'de> {
    pub name: &'de str,
    pub args: ArrayReader<'de>,
}

/// A borrowed reader over msgpack array elements.
pub struct ArrayReader<'de> {
    reader: MsgpackReader<'de>,
    remaining: u32,
}

impl<'de> ArrayReader<'de> {
    #[inline]
    pub fn new(input: &'de [u8]) -> RedrawDecodeResult<Self> {
        let mut reader = MsgpackReader::new(input);
        reader.read_array_reader()
    }

    #[inline]
    #[must_use]
    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    #[inline]
    pub fn read_str(&mut self) -> RedrawDecodeResult<&'de str> {
        self.ensure_remaining()?;
        let value = self.reader.read_str()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_u32(&mut self) -> RedrawDecodeResult<u32> {
        self.ensure_remaining()?;
        let value = self.reader.read_u32()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_u64(&mut self) -> RedrawDecodeResult<u64> {
        self.ensure_remaining()?;
        let value = self.reader.read_u64()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_u32_or_nil(&mut self) -> RedrawDecodeResult<Option<u32>> {
        self.ensure_remaining()?;
        let value = self.reader.read_u32_or_nil()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_i64(&mut self) -> RedrawDecodeResult<i64> {
        self.ensure_remaining()?;
        let value = self.reader.read_i64()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_usize(&mut self) -> RedrawDecodeResult<usize> {
        self.ensure_remaining()?;
        let value = self.reader.read_usize()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_bool(&mut self) -> RedrawDecodeResult<bool> {
        self.ensure_remaining()?;
        let value = self.reader.read_bool()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_f32(&mut self) -> RedrawDecodeResult<f32> {
        self.ensure_remaining()?;
        let value = self.reader.read_f32()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_f64(&mut self) -> RedrawDecodeResult<f64> {
        self.ensure_remaining()?;
        let value = self.reader.read_f64()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_as_string(&mut self) -> RedrawDecodeResult<Option<String>> {
        self.ensure_remaining()?;
        let value = self.reader.read_as_string()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_value_ref(&mut self) -> RedrawDecodeResult<ValueRef<'de>> {
        self.ensure_remaining()?;
        let value = self.reader.read_value_ref()?;
        self.remaining -= 1;
        Ok(value)
    }

    #[inline]
    pub fn read_array<T>(
        &mut self,
        f: impl FnOnce(&mut ArrayReader<'de>) -> RedrawDecodeResult<T>,
    ) -> RedrawDecodeResult<T> {
        self.ensure_remaining()?;

        let mut values = self.reader.read_array_reader()?;
        let value = f(&mut values)?;
        values.skip_remaining()?;

        self.reader.position = values.reader.position;
        self.remaining -= 1;

        Ok(value)
    }

    pub fn read_map<T>(
        &mut self,
        f: impl FnOnce(&mut MapReader<'de>) -> RedrawDecodeResult<T>,
    ) -> RedrawDecodeResult<T> {
        self.ensure_remaining()?;

        let mut entries = self.reader.read_map_reader()?;
        let value = f(&mut entries)?;
        entries.skip_remaining()?;

        self.reader.position = entries.reader.position;
        self.remaining -= 1;

        Ok(value)
    }

    #[inline]
    pub fn skip_next(&mut self) -> RedrawDecodeResult<()> {
        self.ensure_remaining()?;
        self.reader.skip_value()?;
        self.remaining -= 1;
        Ok(())
    }

    #[inline]
    pub fn skip_remaining(&mut self) -> RedrawDecodeResult<()> {
        while self.remaining > 0 {
            self.skip_next()?;
        }
        Ok(())
    }

    #[inline]
    fn ensure_remaining(&self) -> RedrawDecodeResult<()> {
        if self.remaining != 0 {
            Ok(())
        } else {
            Err(RedrawDecodeError::Incomplete)
        }
    }
}

/// A borrowed reader over msgpack map entries.
#[derive(Clone)]
pub struct MapReader<'de> {
    reader: MsgpackReader<'de>,
    remaining: u32,
}

impl<'de> MapReader<'de> {
    #[inline]
    #[must_use]
    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    #[inline]
    pub fn read_pair(&mut self) -> RedrawDecodeResult<(&'de str, ValueRef<'de>)> {
        self.ensure_remaining()?;
        let key = self.reader.read_str()?;
        let value = self.reader.read_value_ref()?;
        self.remaining -= 1;
        Ok((key, value))
    }

    #[inline]
    pub fn skip_next(&mut self) -> RedrawDecodeResult<()> {
        self.ensure_remaining()?;
        self.reader.skip_value()?;
        self.reader.skip_value()?;
        self.remaining -= 1;
        Ok(())
    }

    #[inline]
    pub fn skip_remaining(&mut self) -> RedrawDecodeResult<()> {
        while self.remaining > 0 {
            self.skip_next()?;
        }
        Ok(())
    }

    #[inline]
    fn ensure_remaining(&self) -> RedrawDecodeResult<()> {
        if self.remaining != 0 {
            Ok(())
        } else {
            Err(RedrawDecodeError::Incomplete)
        }
    }
}

#[derive(Clone)]
struct MsgpackReader<'de> {
    input: &'de [u8],
    position: usize,
}

impl<'de> MsgpackReader<'de> {
    #[inline]
    fn new(input: &'de [u8]) -> Self {
        Self { input, position: 0 }
    }

    #[inline]
    fn remaining_slice(&self) -> &'de [u8] {
        &self.input[self.position..]
    }

    #[inline]
    fn read_rmp<T, E>(
        &mut self,
        read: impl FnOnce(&mut Bytes<'de>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut bytes = Bytes::new(self.remaining_slice());
        let value = read(&mut bytes)?;
        self.position += bytes.position() as usize;
        Ok(value)
    }

    fn read_str(&mut self) -> RedrawDecodeResult<&'de str> {
        let (value, tail) = decode::read_str_from_slice(self.remaining_slice())?;
        self.position = self.input.len() - tail.len();
        Ok(value)
    }

    fn read_str_eq(&mut self, expected: &str) -> RedrawDecodeResult<bool> {
        match decode::read_str_from_slice(self.remaining_slice()) {
            Ok((value, tail)) => {
                self.position = self.input.len() - tail.len();
                Ok(value == expected)
            }
            Err(DecodeStringError::TypeMismatch(_)) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    fn read_u32(&mut self) -> RedrawDecodeResult<u32> {
        Ok(self.read_rmp(decode::read_int::<u32, _>)?)
    }

    #[inline]
    fn read_u64(&mut self) -> RedrawDecodeResult<u64> {
        Ok(self.read_rmp(decode::read_int::<u64, _>)?)
    }

    #[inline]
    fn read_usize(&mut self) -> RedrawDecodeResult<usize> {
        Ok(self.read_rmp(decode::read_int::<usize, _>)?)
    }

    #[inline]
    fn read_u32_or_nil(&mut self) -> RedrawDecodeResult<Option<u32>> {
        let mut bytes = Bytes::new(self.remaining_slice());
        if decode::read_marker(&mut bytes)? == Marker::Null {
            self.skip_bytes(1)?;
            Ok(None)
        } else {
            self.read_u32().map(Some)
        }
    }

    #[inline]
    fn read_i64(&mut self) -> RedrawDecodeResult<i64> {
        Ok(self.read_rmp(decode::read_int::<i64, _>)?)
    }

    #[inline]
    fn read_bool(&mut self) -> RedrawDecodeResult<bool> {
        Ok(self.read_rmp(decode::read_bool)?)
    }

    #[inline]
    fn read_f32(&mut self) -> RedrawDecodeResult<f32> {
        Ok(self.read_rmp(decode::read_f32)?)
    }

    #[inline]
    fn read_f64(&mut self) -> RedrawDecodeResult<f64> {
        Ok(self.read_rmp(decode::read_f64)?)
    }

    #[inline]
    fn read_array_len(&mut self) -> RedrawDecodeResult<u32> {
        Ok(self.read_rmp(decode::read_array_len)?)
    }

    #[inline]
    fn read_array_reader(&mut self) -> RedrawDecodeResult<ArrayReader<'de>> {
        self.read_array_len().map(|remaining| ArrayReader {
            reader: self.clone(),
            remaining,
        })
    }

    #[inline]
    fn read_map_reader(&mut self) -> RedrawDecodeResult<MapReader<'de>> {
        let remaining = self.read_rmp(decode::read_map_len)?;
        Ok(MapReader {
            reader: self.clone(),
            remaining,
        })
    }

    #[inline]
    fn read_value_ref(&mut self) -> RedrawDecodeResult<ValueRef<'de>> {
        let mut bytes = self.remaining_slice();
        let before = bytes.len();
        let value = read_value_ref(&mut bytes)?;
        self.position += before - bytes.len();
        Ok(value)
    }

    fn read_as_string(&mut self) -> RedrawDecodeResult<Option<String>> {
        let start = self.position;

        match self.read_rmp(decode::read_marker)? {
            Marker::FixPos(value) => Ok(Some(value.to_string())),
            Marker::FixNeg(value) => Ok(Some(value.to_string())),
            Marker::False => Ok(Some(false.to_string())),
            Marker::True => Ok(Some(true.to_string())),
            Marker::U8 => Ok(Some(self.read_data_u8()?.to_string())),
            Marker::U16 => Ok(Some(self.read_data_u16()?.to_string())),
            Marker::U32 => Ok(Some(self.read_data_u32()?.to_string())),
            Marker::U64 => Ok(Some(self.read_data_u64()?.to_string())),
            Marker::I8 => Ok(Some(self.read_data_i8()?.to_string())),
            Marker::I16 => Ok(Some(self.read_data_i16()?.to_string())),
            Marker::I32 => Ok(Some(self.read_data_i32()?.to_string())),
            Marker::I64 => Ok(Some(self.read_data_i64()?.to_string())),
            Marker::F32 => Ok(Some(self.read_data_f32()?.to_string())),
            Marker::F64 => Ok(Some(self.read_data_f64()?.to_string())),
            Marker::FixStr(_) | Marker::Str8 | Marker::Str16 | Marker::Str32 => {
                self.position = start;
                Ok(Some(self.read_str()?.to_owned()))
            }
            _ => {
                self.position = start;
                self.skip_value()?;
                Ok(None)
            }
        }
    }

    #[inline]
    fn skip_value(&mut self) -> RedrawDecodeResult<()> {
        let consumed = skip_value(self.remaining_slice())?;
        self.position += consumed;
        Ok(())
    }

    #[inline]
    fn skip_values(&mut self, count: u32) -> RedrawDecodeResult<()> {
        for _ in 0..count {
            self.skip_value()?;
        }
        Ok(())
    }

    #[inline]
    fn read_data_u8(&mut self) -> RedrawDecodeResult<u8> {
        Ok(self.read_rmp(RmpRead::read_data_u8)?)
    }

    #[inline]
    fn read_data_u16(&mut self) -> RedrawDecodeResult<u16> {
        Ok(self.read_rmp(RmpRead::read_data_u16)?)
    }

    #[inline]
    fn read_data_u32(&mut self) -> RedrawDecodeResult<u32> {
        Ok(self.read_rmp(RmpRead::read_data_u32)?)
    }

    #[inline]
    fn read_data_u64(&mut self) -> RedrawDecodeResult<u64> {
        Ok(self.read_rmp(RmpRead::read_data_u64)?)
    }

    #[inline]
    fn read_data_i8(&mut self) -> RedrawDecodeResult<i8> {
        Ok(self.read_rmp(RmpRead::read_data_i8)?)
    }

    #[inline]
    fn read_data_i16(&mut self) -> RedrawDecodeResult<i16> {
        Ok(self.read_rmp(RmpRead::read_data_i16)?)
    }

    #[inline]
    fn read_data_i32(&mut self) -> RedrawDecodeResult<i32> {
        Ok(self.read_rmp(RmpRead::read_data_i32)?)
    }

    #[inline]
    fn read_data_i64(&mut self) -> RedrawDecodeResult<i64> {
        Ok(self.read_rmp(RmpRead::read_data_i64)?)
    }

    #[inline]
    fn read_data_f32(&mut self) -> RedrawDecodeResult<f32> {
        Ok(self.read_rmp(RmpRead::read_data_f32)?)
    }

    #[inline]
    fn read_data_f64(&mut self) -> RedrawDecodeResult<f64> {
        Ok(self.read_rmp(RmpRead::read_data_f64)?)
    }

    fn skip_bytes(&mut self, len: usize) -> RedrawDecodeResult<()> {
        let Some(end) = self.position.checked_add(len) else {
            let err = RedrawDecodeError::Invalid("msgpack cursor overflow".into());
            return Err(err);
        };
        if end > self.input.len() {
            return Err(RedrawDecodeError::Incomplete);
        }
        self.position = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::{Value, encode::write_value};

    fn encode_value(value: Value) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_value(&mut bytes, &value).unwrap();
        bytes
    }

    fn redraw_notification(params: Vec<Value>) -> Vec<u8> {
        encode_value(Value::from(vec![
            Value::from(2),
            Value::from("redraw"),
            Value::from(params),
        ]))
    }

    fn rpc_message(fields: Vec<Value>) -> Vec<u8> {
        encode_value(Value::from(fields))
    }

    fn read_redraw_notification(bytes: &[u8]) -> RedrawNotification<'_> {
        let mut reader = MsgpackReader::new(bytes);
        reader.read_rmp(decode::read_array_len).unwrap();
        reader.skip_value().unwrap();
        assert_eq!(reader.read_str().unwrap(), "redraw");
        RedrawNotification::new(reader.read_array_reader().unwrap())
    }

    #[track_caller]
    fn assert_reader_error_kind<T>(result: RedrawDecodeResult<T>, kind: ErrorKind) {
        let err = result.err().expect("expected reader error");
        let err = DecodeError::from(err);
        assert!(matches!(err, DecodeError::ReaderError(ref err) if err.kind() == kind));
    }

    #[track_caller]
    fn assert_incomplete<T>(result: RedrawDecodeResult<T>) {
        assert!(matches!(result, Err(RedrawDecodeError::Incomplete)));
    }

    #[track_caller]
    fn skip_ok(bytes: Vec<u8>) {
        let len = bytes.len();
        let mut reader = MsgpackReader::new(&bytes);
        reader.skip_value().unwrap();
        assert_eq!(reader.position, len);
    }

    fn push_u16(bytes: &mut Vec<u8>, value: u16) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_u32(bytes: &mut Vec<u8>, value: u32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn encoded_f32(value: f32) -> Vec<u8> {
        let mut bytes = vec![Marker::F32.to_u8()];
        bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        bytes
    }

    fn encoded_f64(value: f64) -> Vec<u8> {
        let mut bytes = vec![Marker::F64.to_u8()];
        bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        bytes
    }

    fn fixed_payload(marker: Marker, len: usize) -> Vec<u8> {
        let mut bytes = vec![marker.to_u8(), 0];
        bytes.extend(std::iter::repeat_n(1, len));
        bytes
    }

    fn len8_payload(marker: Marker, len: u8, includes_ext_type: bool) -> Vec<u8> {
        let mut bytes = vec![marker.to_u8(), len];
        if includes_ext_type {
            bytes.push(0);
        }
        bytes.extend(std::iter::repeat_n(1, len as usize));
        bytes
    }

    fn len16_payload(marker: Marker, len: u16, includes_ext_type: bool) -> Vec<u8> {
        let mut bytes = vec![marker.to_u8()];
        push_u16(&mut bytes, len);
        if includes_ext_type {
            bytes.push(0);
        }
        bytes.extend(std::iter::repeat_n(1, len as usize));
        bytes
    }

    fn len32_payload(marker: Marker, len: u32, includes_ext_type: bool) -> Vec<u8> {
        let mut bytes = vec![marker.to_u8()];
        push_u32(&mut bytes, len);
        if includes_ext_type {
            bytes.push(0);
        }
        bytes.extend(std::iter::repeat_n(1, len as usize));
        bytes
    }

    #[test]
    fn redraw_frame_probe_rejects_non_redraw_messages() {
        assert!(
            RedrawFrameInfo::probe(&encode_value(Value::from("redraw")))
                .unwrap()
                .is_none()
        );
        assert!(
            RedrawFrameInfo::probe(&rpc_message(Vec::new()))
                .unwrap()
                .is_none()
        );
        assert!(
            RedrawFrameInfo::probe(&rpc_message(vec![Value::from(2)]))
                .unwrap()
                .is_none()
        );

        let request = rpc_message(vec![
            Value::from(0),
            Value::from(7),
            Value::from("redraw"),
            Value::from(Vec::<Value>::new()),
        ]);
        assert!(RedrawFrameInfo::probe(&request).unwrap().is_none());

        let non_integer_message_type = rpc_message(vec![
            Value::from("notification"),
            Value::from("redraw"),
            Value::from(Vec::<Value>::new()),
        ]);
        assert!(
            RedrawFrameInfo::probe(&non_integer_message_type)
                .unwrap()
                .is_none()
        );

        let response = rpc_message(vec![
            Value::from(1),
            Value::from(7),
            Value::Nil,
            Value::from(true),
        ]);
        assert!(RedrawFrameInfo::probe(&response).unwrap().is_none());

        let non_redraw = rpc_message(vec![
            Value::from(2),
            Value::from("not-redraw"),
            Value::from(Vec::<Value>::new()),
        ]);
        assert!(RedrawFrameInfo::probe(&non_redraw).unwrap().is_none());

        let non_string_method = rpc_message(vec![
            Value::from(2),
            Value::from(7),
            Value::from(Vec::<Value>::new()),
        ]);
        assert!(
            RedrawFrameInfo::probe(&non_string_method)
                .unwrap()
                .is_none()
        );

        let method_only = rpc_message(vec![Value::from(2), Value::from("redraw")]);
        assert!(RedrawFrameInfo::probe(&method_only).unwrap().is_none());

        let non_array_params = rpc_message(vec![
            Value::from(2),
            Value::from("redraw"),
            Value::from("not-array"),
        ]);
        assert!(RedrawFrameInfo::probe(&non_array_params).unwrap().is_none());
    }

    #[test]
    fn redraw_frame_probe_reports_complete_incomplete_and_not_redraw() {
        let redraw = redraw_notification(vec![Value::from(vec![Value::from("flush")])]);
        let incomplete_redraw_prefix = [
            Marker::FixArray(3).to_u8(),
            2,
            Marker::FixStr(6).to_u8(),
            b'r',
            b'e',
            b'd',
            b'r',
            b'a',
            b'w',
        ];
        let request = rpc_message(vec![
            Value::from(0),
            Value::from(1),
            Value::from("method"),
            Value::from(Vec::<Value>::new()),
        ]);

        let info = RedrawFrameInfo::probe(&redraw)
            .unwrap()
            .expect("redraw frame");
        assert_eq!(info.consumed(), redraw.len());
        let frame = RedrawFrame::from_slice(&redraw).unwrap();
        assert_eq!(frame.as_bytes(), redraw.as_slice());
        assert_incomplete(RedrawFrameInfo::probe(&[]));
        assert!(matches!(
            RedrawFrameInfo::probe(&incomplete_redraw_prefix),
            Err(RedrawDecodeError::Incomplete)
        ));
        assert!(RedrawFrameInfo::probe(&request).unwrap().is_none());
    }

    #[test]
    fn redraw_frame_probe_counts_extra_outer_fields() {
        let redraw = rpc_message(vec![
            Value::from(2),
            Value::from("redraw"),
            Value::from(vec![Value::from(vec![Value::from("flush")])]),
            Value::from("extra"),
        ]);
        let mut bytes = redraw.clone();
        bytes.extend_from_slice(&encode_value(Value::from("tail")));

        let info = RedrawFrameInfo::probe(&bytes)
            .unwrap()
            .expect("redraw frame");
        assert_eq!(info.consumed(), redraw.len());
    }

    #[test]
    fn redraw_frame_notification_reads_params_from_probe_offset() {
        let redraw = redraw_notification(vec![
            Value::from(vec![
                Value::from("grid_resize"),
                Value::from(vec![Value::from(1), Value::from(80), Value::from(24)]),
            ]),
            Value::from(vec![Value::from("flush")]),
        ]);
        let frame = RedrawFrame::from_slice(&redraw).unwrap();
        let mut notification = frame.notification().unwrap();
        let mut seen = Vec::new();

        assert_eq!(notification.batch_count(), 2);
        notification
            .for_each_batch(|batch| {
                seen.push(batch.name.to_owned());

                if batch.name == "grid_resize" {
                    batch.args.read_array(|args| {
                        assert_eq!(args.read_u64()?, 1);
                        assert_eq!(args.read_u64()?, 80);
                        assert_eq!(args.read_u64()?, 24);
                        Ok(true)
                    })?;
                } else {
                    assert!(batch.args.is_empty());
                }

                Ok(true)
            })
            .unwrap();

        assert_eq!(seen, vec!["grid_resize", "flush"]);
    }

    #[test]
    fn redraw_frame_probe_reports_malformed_method_payloads() {
        let invalid_utf8_method = vec![
            Marker::FixArray(3).to_u8(),
            2,
            Marker::Str8.to_u8(),
            1,
            0xff,
            Marker::FixArray(0).to_u8(),
        ];
        assert_reader_error_kind(
            RedrawFrameInfo::probe(&invalid_utf8_method),
            ErrorKind::InvalidData,
        );

        let incomplete_method = vec![
            Marker::FixArray(3).to_u8(),
            2,
            Marker::Str8.to_u8(),
            2,
            b'a',
        ];
        assert_incomplete(RedrawFrameInfo::probe(&incomplete_method));

        let incomplete_first_item = vec![Marker::FixArray(3).to_u8(), Marker::U16.to_u8(), 1];
        assert_incomplete(RedrawFrameInfo::probe(&incomplete_first_item));
    }

    #[test]
    fn redraw_notification_reads_batches_and_args() {
        let bytes = redraw_notification(vec![
            Value::from(vec![
                Value::from("grid_line"),
                Value::from(vec![Value::from(1), Value::from(-2), Value::from(true)]),
            ]),
            Value::from(vec![Value::from("flush")]),
        ]);
        let mut redraw = read_redraw_notification(&bytes);
        let mut seen = Vec::new();
        assert_eq!(redraw.batch_count(), 2);

        redraw
            .for_each_batch(|batch| {
                seen.push(batch.name.to_owned());

                if batch.name == "grid_line" {
                    assert_eq!(batch.args.remaining(), 1);
                    while !batch.args.is_empty() {
                        batch.args.read_array(|args| {
                            assert_eq!(args.read_u64()?, 1);
                            assert_eq!(args.read_i64()?, -2);
                            assert!(args.read_bool()?);
                            Ok(())
                        })?;
                    }
                } else {
                    assert!(batch.args.is_empty());
                }

                Ok(true)
            })
            .unwrap();

        assert_eq!(seen, vec!["grid_line", "flush"]);
    }

    #[test]
    fn redraw_notification_reports_invalid_batch_name() {
        let bytes = redraw_notification(vec![Value::from(vec![Value::from(1)])]);
        let mut redraw = read_redraw_notification(&bytes);

        assert_reader_error_kind(
            redraw.for_each_batch(|_| unreachable!()),
            ErrorKind::InvalidData,
        );
    }

    #[test]
    fn redraw_batch_skips_unread_args() {
        let bytes = redraw_notification(vec![
            Value::from(vec![
                Value::from("grid_line"),
                Value::from(vec![Value::from(1)]),
                Value::from(vec![Value::from(2)]),
            ]),
            Value::from(vec![Value::from("flush")]),
        ]);
        let mut redraw = read_redraw_notification(&bytes);
        let mut names = Vec::new();

        redraw
            .for_each_batch(|batch| {
                names.push(batch.name.to_owned());
                Ok(true)
            })
            .unwrap();

        assert_eq!(names, vec!["grid_line", "flush"]);
    }

    #[test]
    fn redraw_notification_stops_when_batch_callback_returns_false() {
        let bytes = redraw_notification(vec![
            Value::from(vec![
                Value::from("grid_line"),
                Value::from(vec![Value::from(1)]),
            ]),
            Value::from(vec![Value::from("flush")]),
        ]);
        let mut redraw = read_redraw_notification(&bytes);
        let mut names = Vec::new();

        redraw
            .for_each_batch(|batch| {
                names.push(batch.name.to_owned());
                Ok(false)
            })
            .unwrap();

        assert_eq!(names, vec!["grid_line"]);
        assert_eq!(redraw.batch_count(), 1);

        redraw
            .for_each_batch(|batch| {
                names.push(batch.name.to_owned());
                Ok(true)
            })
            .unwrap();

        assert_eq!(names, vec!["grid_line", "flush"]);
        assert_eq!(redraw.batch_count(), 0);
    }

    #[test]
    fn array_reader_reads_float_values() {
        let f32_value = 1.25_f32;
        let f64_value = -2.5_f64;
        let mut bytes = vec![Marker::FixArray(2).to_u8()];
        bytes.extend_from_slice(&encoded_f32(f32_value));
        bytes.extend_from_slice(&encoded_f64(f64_value));

        let mut reader = MsgpackReader::new(&bytes);
        let mut array = reader.read_array_reader().unwrap();
        assert_eq!(array.read_f32().unwrap(), f32_value);
        assert_eq!(array.read_f64().unwrap(), f64_value);
        assert!(array.is_empty());
    }

    #[test]
    fn array_reader_new_reads_array_payload() {
        let bytes = encode_value(Value::from(vec![Value::from(24), Value::from("text")]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_eq!(array.remaining(), 2);
        assert_eq!(array.read_u64().unwrap(), 24);
        assert_eq!(array.read_str().unwrap(), "text");
        assert!(array.is_empty());
    }

    #[test]
    fn array_reader_reads_u32_values() {
        let mut bytes = vec![Marker::FixArray(4).to_u8()];
        bytes.push(Marker::FixPos(24).to_u8());
        bytes.push(Marker::U16.to_u8());
        push_u16(&mut bytes, 256);
        bytes.push(Marker::U32.to_u8());
        push_u32(&mut bytes, 65_536);
        bytes.push(Marker::I8.to_u8());
        bytes.extend_from_slice(&2_i8.to_be_bytes());

        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_eq!(array.read_u32().unwrap(), 24);
        assert_eq!(array.read_u32().unwrap(), 256);
        assert_eq!(array.read_u32().unwrap(), 65_536);
        assert_eq!(array.read_u32().unwrap(), 2);
        assert!(array.is_empty());
        assert_incomplete(array.read_u32());
    }

    #[test]
    fn array_reader_reports_u32_errors() {
        let bytes = encode_value(Value::from(vec![Value::from("not-u32")]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32(), ErrorKind::InvalidData);

        let bytes = encode_value(Value::from(vec![Value::from(-1)]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32(), ErrorKind::InvalidData);

        let mut bytes = vec![Marker::FixArray(1).to_u8(), Marker::U64.to_u8()];
        push_u64(&mut bytes, u64::from(u32::MAX) + 1);
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32(), ErrorKind::InvalidData);

        let bytes = vec![Marker::FixArray(1).to_u8(), Marker::U16.to_u8()];
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_incomplete(array.read_u32());
    }

    #[test]
    fn array_reader_reads_usize_values() {
        let mut bytes = vec![Marker::FixArray(4).to_u8()];
        bytes.push(Marker::FixPos(24).to_u8());
        bytes.push(Marker::U16.to_u8());
        push_u16(&mut bytes, 256);
        bytes.push(Marker::U32.to_u8());
        push_u32(&mut bytes, 65_536);
        bytes.push(Marker::I8.to_u8());
        bytes.extend_from_slice(&2_i8.to_be_bytes());

        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_eq!(array.read_usize().unwrap(), 24);
        assert_eq!(array.read_usize().unwrap(), 256);
        assert_eq!(array.read_usize().unwrap(), 65_536);
        assert_eq!(array.read_usize().unwrap(), 2);
        assert!(array.is_empty());
        assert_incomplete(array.read_usize());
    }

    #[test]
    fn array_reader_reports_usize_errors() {
        let bytes = encode_value(Value::from(vec![Value::from("not-usize")]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_usize(), ErrorKind::InvalidData);

        let bytes = encode_value(Value::from(vec![Value::from(-1)]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_usize(), ErrorKind::InvalidData);

        let bytes = vec![Marker::FixArray(1).to_u8(), Marker::U16.to_u8()];
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_incomplete(array.read_usize());
    }

    #[test]
    fn array_reader_reads_u32_or_nil_values() {
        let mut bytes = vec![Marker::FixArray(5).to_u8()];
        bytes.push(Marker::FixPos(24).to_u8());
        bytes.push(Marker::Null.to_u8());
        bytes.push(Marker::U16.to_u8());
        push_u16(&mut bytes, 256);
        bytes.push(Marker::U32.to_u8());
        push_u32(&mut bytes, u32::MAX);
        bytes.push(Marker::I8.to_u8());
        bytes.extend_from_slice(&2_i8.to_be_bytes());

        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_eq!(array.read_u32_or_nil().unwrap(), Some(24));
        assert_eq!(array.read_u32_or_nil().unwrap(), None);
        assert_eq!(array.read_u32_or_nil().unwrap(), Some(256));
        assert_eq!(array.read_u32_or_nil().unwrap(), Some(u32::MAX));
        assert_eq!(array.read_u32_or_nil().unwrap(), Some(2));
        assert!(array.is_empty());
        assert_incomplete(array.read_u32_or_nil());
    }

    #[test]
    fn array_reader_reports_u32_or_nil_errors() {
        let bytes = encode_value(Value::from(vec![Value::from("not-u32")]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32_or_nil(), ErrorKind::InvalidData);

        let bytes = encode_value(Value::from(vec![Value::from(-1)]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32_or_nil(), ErrorKind::InvalidData);

        let mut bytes = vec![Marker::FixArray(1).to_u8(), Marker::U64.to_u8()];
        push_u64(&mut bytes, u64::from(u32::MAX) + 1);
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_reader_error_kind(array.read_u32_or_nil(), ErrorKind::InvalidData);

        let bytes = vec![Marker::FixArray(1).to_u8(), Marker::U16.to_u8()];
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_incomplete(array.read_u32_or_nil());
    }

    #[test]
    fn array_reader_stringifies_scalar_values() {
        let f32_value = 1.25_f32;
        let f64_value = -2.5_f64;
        let mut bytes = vec![Marker::Array16.to_u8()];
        push_u16(&mut bytes, 18);
        bytes.push(Marker::FixPos(24).to_u8());
        bytes.push(Marker::FixNeg(-1).to_u8());
        bytes.extend_from_slice(&[Marker::U8.to_u8(), 255]);
        bytes.push(Marker::U16.to_u8());
        push_u16(&mut bytes, 256);
        bytes.push(Marker::U32.to_u8());
        push_u32(&mut bytes, 65_536);
        bytes.push(Marker::U64.to_u8());
        push_u64(&mut bytes, 9_007_199_254_740_991);
        bytes.push(Marker::I8.to_u8());
        bytes.extend_from_slice(&(-2_i8).to_be_bytes());
        bytes.push(Marker::I16.to_u8());
        bytes.extend_from_slice(&(-257_i16).to_be_bytes());
        bytes.push(Marker::I32.to_u8());
        bytes.extend_from_slice(&(-65_537_i32).to_be_bytes());
        bytes.push(Marker::I64.to_u8());
        bytes.extend_from_slice(&(-2_147_483_649_i64).to_be_bytes());
        bytes.push(Marker::True.to_u8());
        bytes.push(Marker::False.to_u8());
        bytes.extend_from_slice(&encoded_f32(f32_value));
        bytes.extend_from_slice(&encoded_f64(f64_value));
        bytes.push(Marker::FixStr(4).to_u8());
        bytes.extend_from_slice(b"text");
        bytes.push(Marker::Null.to_u8());
        bytes.push(Marker::FixArray(1).to_u8());
        bytes.push(Marker::FixPos(1).to_u8());
        bytes.push(Marker::FixMap(1).to_u8());
        bytes.push(Marker::Null.to_u8());
        bytes.push(Marker::True.to_u8());

        let mut reader = MsgpackReader::new(&bytes);
        let mut array = reader.read_array_reader().unwrap();

        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("24"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("-1"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("255"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("256"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("65536"));
        assert_eq!(
            array.read_as_string().unwrap().as_deref(),
            Some("9007199254740991")
        );
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("-2"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("-257"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("-65537"));
        assert_eq!(
            array.read_as_string().unwrap().as_deref(),
            Some("-2147483649")
        );
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("true"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("false"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("1.25"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("-2.5"));
        assert_eq!(array.read_as_string().unwrap().as_deref(), Some("text"));
        assert_eq!(array.read_as_string().unwrap(), None);
        assert_eq!(array.read_as_string().unwrap(), None);
        assert_eq!(array.read_as_string().unwrap(), None);
        assert!(array.is_empty());
        assert_incomplete(array.read_as_string());
    }

    #[test]
    fn array_reader_reads_value_refs() {
        let bytes = encode_value(Value::from(vec![
            Value::from("text"),
            Value::from(vec![Value::from(1), Value::from(true)]),
            Value::from(vec![(Value::from("key"), Value::from("value"))]),
        ]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert!(
            matches!(array.read_value_ref().unwrap(), ValueRef::String(value) if value.as_str() == Some("text"))
        );
        assert!(
            matches!(array.read_value_ref().unwrap(), ValueRef::Array(values) if values.len() == 2)
        );
        assert!(
            matches!(array.read_value_ref().unwrap(), ValueRef::Map(entries) if entries.len() == 1)
        );
        assert_incomplete(array.read_value_ref());
    }

    #[test]
    fn array_reader_reads_map_values() {
        let bytes = encode_value(Value::from(vec![Value::from(vec![(
            Value::from("k"),
            Value::from(1),
        )])]));
        let mut array = ArrayReader::new(&bytes).unwrap();

        assert_eq!(array.read_map(|map| Ok(map.remaining())).unwrap(), 1);
        assert!(array.is_empty());
    }

    #[test]
    fn map_reader_reads_string_key_value_ref_pairs() {
        let bytes = encode_value(Value::from(vec![
            (Value::from("k1"), Value::from(1)),
            (Value::from("k2"), Value::from(true)),
            (Value::from("k3"), Value::from("v")),
        ]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut map = reader.read_map_reader().unwrap();

        let (key, value) = map.read_pair().unwrap();
        assert_eq!(key, "k1");
        assert_eq!(value.as_u64(), Some(1));

        let (key, value) = map.read_pair().unwrap();
        assert_eq!(key, "k2");
        assert!(matches!(value, ValueRef::Boolean(true)));

        let (key, value) = map.read_pair().unwrap();
        assert_eq!(key, "k3");
        assert!(matches!(value, ValueRef::String(value) if value.as_str() == Some("v")));

        assert!(map.is_empty());
        assert_reader_error_kind(map.read_pair(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn map_reader_read_pair_reports_non_string_key() {
        let bytes = encode_value(Value::from(vec![(Value::from(1), Value::from(true))]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut map = reader.read_map_reader().unwrap();

        assert_reader_error_kind(map.read_pair(), ErrorKind::InvalidData);
    }

    #[test]
    fn array_reader_reads_values_and_reports_boundaries() {
        let bytes = encode_value(Value::from(vec![Value::from("value")]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut array = reader.read_array_reader().unwrap();

        assert_eq!(array.read_str().unwrap(), "value");
        assert_reader_error_kind(array.read_str(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn array_reader_reports_type_errors() {
        let bytes = encode_value(Value::from(vec![
            Value::from("not-u64"),
            Value::from("not-i64"),
            Value::from(1),
            Value::from(1),
            Value::from(1),
        ]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut array = reader.read_array_reader().unwrap();

        assert_reader_error_kind(array.read_u64(), ErrorKind::InvalidData);
        assert_reader_error_kind(array.read_i64(), ErrorKind::InvalidData);
        assert_reader_error_kind(array.read_bool(), ErrorKind::InvalidData);
        assert_reader_error_kind(array.read_array(|_| Ok(())), ErrorKind::InvalidData);
        assert_reader_error_kind(array.read_map(|_| Ok(())), ErrorKind::InvalidData);
    }

    #[test]
    fn map_reader_skips_entries() {
        let bytes = encode_value(Value::from(vec![
            (Value::from("k1"), Value::from(1)),
            (Value::from("k2"), Value::from(2)),
        ]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut map = reader.read_map_reader().unwrap();

        assert_eq!(map.remaining(), 2);
        assert!(!map.is_empty());

        map.skip_next().unwrap();
        assert_eq!(map.remaining(), 1);

        let (key, value) = map.read_pair().unwrap();
        assert_eq!(key, "k2");
        assert_eq!(value.as_u64(), Some(2));

        assert!(map.is_empty());
        assert_reader_error_kind(map.skip_next(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn map_reader_skips_remaining_entries() {
        let bytes = encode_value(Value::from(vec![(Value::from("k"), Value::from("v"))]));
        let mut reader = MsgpackReader::new(&bytes);
        let mut map = reader.read_map_reader().unwrap();

        map.skip_remaining().unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn msgpack_reader_skips_all_payload_marker_families() {
        skip_ok(vec![Marker::FixPos(1).to_u8()]);
        skip_ok(vec![Marker::FixNeg(-1).to_u8()]);
        skip_ok(vec![Marker::Null.to_u8()]);
        skip_ok(vec![Marker::False.to_u8()]);
        skip_ok(vec![Marker::True.to_u8()]);
        skip_ok(vec![Marker::FixStr(1).to_u8(), b'a']);
        skip_ok(vec![Marker::FixArray(1).to_u8(), Marker::Null.to_u8()]);
        skip_ok(vec![
            Marker::FixMap(1).to_u8(),
            Marker::Null.to_u8(),
            Marker::True.to_u8(),
        ]);

        skip_ok(len8_payload(Marker::Bin8, 1, false));
        skip_ok(len16_payload(Marker::Bin16, 1, false));
        skip_ok(len32_payload(Marker::Bin32, 1, false));
        skip_ok(len8_payload(Marker::Ext8, 1, true));
        skip_ok(len16_payload(Marker::Ext16, 1, true));
        skip_ok(len32_payload(Marker::Ext32, 1, true));
        skip_ok(vec![Marker::F32.to_u8(), 0, 0, 0, 0]);
        skip_ok(vec![Marker::F64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 0]);
        skip_ok(vec![Marker::U8.to_u8(), 1]);
        skip_ok(vec![Marker::I8.to_u8(), 1]);
        skip_ok(vec![Marker::U16.to_u8(), 0, 1]);
        skip_ok(vec![Marker::I16.to_u8(), 0, 1]);
        skip_ok(vec![Marker::U32.to_u8(), 0, 0, 0, 1]);
        skip_ok(vec![Marker::I32.to_u8(), 0, 0, 0, 1]);
        skip_ok(vec![Marker::U64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 1]);
        skip_ok(vec![Marker::I64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 1]);
        skip_ok(fixed_payload(Marker::FixExt1, 1));
        skip_ok(fixed_payload(Marker::FixExt2, 2));
        skip_ok(fixed_payload(Marker::FixExt4, 4));
        skip_ok(fixed_payload(Marker::FixExt8, 8));
        skip_ok(fixed_payload(Marker::FixExt16, 16));
        skip_ok(len8_payload(Marker::Str8, 1, false));
        skip_ok(len16_payload(Marker::Str16, 1, false));
        skip_ok(len32_payload(Marker::Str32, 1, false));

        let mut array16 = vec![Marker::Array16.to_u8()];
        push_u16(&mut array16, 1);
        array16.push(Marker::Null.to_u8());
        skip_ok(array16);

        let mut array32 = vec![Marker::Array32.to_u8()];
        push_u32(&mut array32, 1);
        array32.push(Marker::Null.to_u8());
        skip_ok(array32);

        let mut map16 = vec![Marker::Map16.to_u8()];
        push_u16(&mut map16, 1);
        map16.push(Marker::Null.to_u8());
        map16.push(Marker::True.to_u8());
        skip_ok(map16);

        let mut map32 = vec![Marker::Map32.to_u8()];
        push_u32(&mut map32, 1);
        map32.push(Marker::Null.to_u8());
        map32.push(Marker::True.to_u8());
        skip_ok(map32);
    }

    #[test]
    fn msgpack_reader_reports_skip_errors() {
        let reserved = [Marker::Reserved.to_u8()];
        let mut reader = MsgpackReader::new(&reserved);
        assert_reader_error_kind(reader.skip_value(), ErrorKind::InvalidData);

        let mut reader = MsgpackReader::new(&[]);
        assert_reader_error_kind(reader.skip_value(), ErrorKind::UnexpectedEof);

        let incomplete_bin = [Marker::Bin8.to_u8()];
        let mut reader = MsgpackReader::new(&incomplete_bin);
        assert_reader_error_kind(reader.skip_value(), ErrorKind::UnexpectedEof);

        let incomplete_fixstr = [Marker::FixStr(2).to_u8(), b'a'];
        let mut reader = MsgpackReader::new(&incomplete_fixstr);
        assert_reader_error_kind(reader.skip_value(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn msgpack_reader_reports_integer_range_errors() {
        let bytes = [
            Marker::U64.to_u8(),
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
            0xff,
        ];
        let mut reader = MsgpackReader::new(&bytes);

        assert_reader_error_kind(reader.read_i64(), ErrorKind::InvalidData);
    }

    #[test]
    fn msgpack_reader_reports_truncated_reads() {
        let mut reader = MsgpackReader::new(&[]);
        assert_reader_error_kind(reader.read_bool(), ErrorKind::UnexpectedEof);

        let mut reader = MsgpackReader::new(&[]);
        assert_reader_error_kind(reader.read_i64(), ErrorKind::UnexpectedEof);

        let truncated_u64 = [Marker::U64.to_u8(), 0];
        let mut reader = MsgpackReader::new(&truncated_u64);
        assert_reader_error_kind(reader.read_i64(), ErrorKind::UnexpectedEof);

        let truncated_array_len = [Marker::Array16.to_u8()];
        let mut reader = MsgpackReader::new(&truncated_array_len);
        assert_reader_error_kind(
            reader.read_array_reader().map(|_| ()),
            ErrorKind::UnexpectedEof,
        );

        let truncated_map_len = [Marker::Map16.to_u8()];
        let mut reader = MsgpackReader::new(&truncated_map_len);
        assert_reader_error_kind(
            reader.read_map_reader().map(|_| ()),
            ErrorKind::UnexpectedEof,
        );

        let truncated_value = [Marker::Str8.to_u8(), 2, b'a'];
        let mut reader = MsgpackReader::new(&truncated_value);
        assert_reader_error_kind(reader.read_value_ref(), ErrorKind::UnexpectedEof);

        let mut reader = MsgpackReader::new(&[]);
        assert_reader_error_kind(reader.read_str().map(|_| ()), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn msgpack_reader_reports_cursor_overflow() {
        let mut reader = MsgpackReader {
            input: &[],
            position: usize::MAX,
        };

        assert_reader_error_kind(reader.skip_bytes(1), ErrorKind::InvalidData);
    }
}
