//! Borrowed msgpack readers for a future redraw notification fast path.

use std::{
  fmt::Debug,
  io::{self, Cursor, ErrorKind},
  sync::Arc,
};

use rmp::{
  Marker,
  decode::{
    self, Bytes, DecodeStringError, NumValueReadError, RmpRead, ValueReadError,
    bytes::BytesReadError,
  },
};
use rmpv::{Value, decode::read_value};

use crate::error::{DecodeError, LoopError};

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

impl From<RedrawDecodeError> for Box<LoopError> {
  fn from(value: RedrawDecodeError) -> Self {
    Box::new(LoopError::DecodeError(Arc::new(value.into()), None))
  }
}

impl From<io::Error> for RedrawDecodeError {
  fn from(err: io::Error) -> Self {
    if err.kind() == ErrorKind::UnexpectedEof {
      Self::Incomplete
    } else {
      Self::new(err)
    }
  }
}

impl From<BytesReadError> for RedrawDecodeError {
  fn from(err: BytesReadError) -> Self {
    match err {
      BytesReadError::InsufficientBytes { .. } => Self::Incomplete,
      err => Self::new(err),
    }
  }
}

impl From<decode::MarkerReadError<BytesReadError>> for RedrawDecodeError {
  fn from(err: decode::MarkerReadError<BytesReadError>) -> Self {
    err.0.into()
  }
}

impl From<ValueReadError<BytesReadError>> for RedrawDecodeError {
  fn from(err: ValueReadError<BytesReadError>) -> Self {
    match err {
      ValueReadError::InvalidMarkerRead(err)
      | ValueReadError::InvalidDataRead(err) => err.into(),
      err => Self::new(err),
    }
  }
}

impl From<ValueReadError<io::Error>> for RedrawDecodeError {
  fn from(err: ValueReadError<io::Error>) -> Self {
    match err {
      ValueReadError::InvalidMarkerRead(err)
      | ValueReadError::InvalidDataRead(err) => err.into(),
      err => Self::new(err),
    }
  }
}

impl From<NumValueReadError<BytesReadError>> for RedrawDecodeError {
  fn from(err: NumValueReadError<BytesReadError>) -> Self {
    match err {
      NumValueReadError::InvalidMarkerRead(err)
      | NumValueReadError::InvalidDataRead(err) => err.into(),
      err => Self::new(err),
    }
  }
}

impl From<DecodeStringError<'_, BytesReadError>> for RedrawDecodeError {
  fn from(err: DecodeStringError<'_, BytesReadError>) -> Self {
    match err {
      DecodeStringError::InvalidMarkerRead(err)
      | DecodeStringError::InvalidDataRead(err) => err.into(),
      DecodeStringError::BufferSizeTooSmall(_) => Self::Incomplete,
      err => Self::new(err),
    }
  }
}

impl From<rmpv::decode::Error> for RedrawDecodeError {
  fn from(err: rmpv::decode::Error) -> Self {
    Self::new(err)
  }
}

/// A complete owned `redraw` notification frame.
#[derive(Clone, PartialEq, Eq)]
pub struct RedrawFrame {
  bytes: Vec<u8>,
  params_offset: usize,
  params_len: u32,
}

impl Debug for RedrawFrame {
  fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    fmt
      .debug_struct("RedrawFrame")
      .field("len", &self.bytes.len())
      .finish()
  }
}

impl RedrawFrame {
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
      Err(NumValueReadError::TypeMismatch(_))
      | Err(NumValueReadError::OutOfRange) => {
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
      bytes: bytes[..reader.position].to_vec(),
      params_offset,
      params_len,
    }))
  }

  pub fn consumed(&self) -> usize {
    self.bytes.len()
  }

  pub fn as_bytes(&self) -> &[u8] {
    &self.bytes
  }

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

/// A single msgpack value borrowed from the input buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawMsgpack<'de> {
  bytes: &'de [u8],
}

impl<'de> RawMsgpack<'de> {
  pub fn as_bytes(&self) -> &'de [u8] {
    self.bytes
  }

  pub fn as_str(&self) -> RedrawDecodeResult<&'de str> {
    Ok(decode::read_str_from_slice(self.bytes)?.0)
  }

  pub fn as_i64(&self) -> RedrawDecodeResult<i64> {
    Ok(decode::read_int::<i64, _>(&mut Bytes::new(self.bytes))?)
  }

  pub fn as_u64(&self) -> RedrawDecodeResult<u64> {
    Ok(decode::read_int::<u64, _>(&mut Bytes::new(self.bytes))?)
  }

  pub fn as_bool(&self) -> RedrawDecodeResult<bool> {
    Ok(decode::read_bool(&mut Bytes::new(self.bytes))?)
  }

  pub fn as_f32(&self) -> RedrawDecodeResult<f32> {
    Ok(decode::read_f32(&mut Bytes::new(self.bytes))?)
  }

  pub fn as_f64(&self) -> RedrawDecodeResult<f64> {
    Ok(decode::read_f64(&mut Bytes::new(self.bytes))?)
  }
}

/// Return whether the msgpack-rpc envelope's method slot is `redraw`.
///
/// This is intentionally narrow: the future fast path only needs to decide
/// whether the next local Neovim message should use the redraw reader.
///
/// `RedrawDecodeError::Incomplete` means the buffer is incomplete before the
/// method and params array header can be checked.
#[cfg(test)]
fn is_redraw_method(bytes: &[u8]) -> RedrawDecodeResult<bool> {
  let mut reader = MsgpackReader::new(bytes);
  let outer_len = match reader.read_rmp(decode::read_array_len) {
    Ok(len) => len,
    Err(ValueReadError::TypeMismatch(_)) => return Ok(false),
    Err(err) => return Err(err.into()),
  };

  if outer_len < 3 {
    return Ok(false);
  }

  let msg_type = match reader.read_rmp(decode::read_int::<u64, _>) {
    Ok(msg_type) => msg_type,
    Err(NumValueReadError::TypeMismatch(_))
    | Err(NumValueReadError::OutOfRange) => return Ok(false),
    Err(err) => return Err(err.into()),
  };

  if msg_type != 2 {
    return Ok(false);
  }

  if !reader.read_str_eq("redraw")? {
    return Ok(false);
  }

  match reader.read_rmp(decode::read_array_len) {
    Ok(_) => Ok(true),
    Err(ValueReadError::TypeMismatch(_)) => Ok(false),
    Err(err) => Err(err.into()),
  }
}

/// The params of a `redraw` notification.
#[derive(Debug, Clone)]
pub struct RedrawNotification<'de> {
  params: ArrayReader<'de>,
}

impl<'de> RedrawNotification<'de> {
  #[must_use]
  pub(crate) fn new(params: ArrayReader<'de>) -> Self {
    Self { params }
  }

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
      let args = batch_items.take_remaining();
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
#[derive(Debug, Clone)]
pub struct RedrawBatch<'de> {
  pub name: &'de str,
  pub args: ArrayReader<'de>,
}

/// A borrowed reader over msgpack array elements.
#[derive(Debug, Clone)]
pub struct ArrayReader<'de> {
  reader: MsgpackReader<'de>,
  remaining: u32,
}

impl<'de> ArrayReader<'de> {
  pub fn new(input: &'de [u8]) -> RedrawDecodeResult<Self> {
    let mut reader = MsgpackReader::new(input);
    reader.read_array_reader()
  }

  #[must_use]
  pub fn remaining(&self) -> u32 {
    self.remaining
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.remaining == 0
  }

  pub fn read_str(&mut self) -> RedrawDecodeResult<&'de str> {
    self.ensure_remaining()?;
    let value = self.reader.read_str()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_u64(&mut self) -> RedrawDecodeResult<u64> {
    self.ensure_remaining()?;
    let value = self.reader.read_u64()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_i64(&mut self) -> RedrawDecodeResult<i64> {
    self.ensure_remaining()?;
    let value = self.reader.read_i64()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_bool(&mut self) -> RedrawDecodeResult<bool> {
    self.ensure_remaining()?;
    let value = self.reader.read_bool()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_f32(&mut self) -> RedrawDecodeResult<f32> {
    self.ensure_remaining()?;
    let value = self.reader.read_f32()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_f64(&mut self) -> RedrawDecodeResult<f64> {
    self.ensure_remaining()?;
    let value = self.reader.read_f64()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_as_string(&mut self) -> RedrawDecodeResult<Option<String>> {
    self.ensure_remaining()?;
    let value = self.reader.read_as_string()?;
    self.remaining -= 1;
    Ok(value)
  }

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

  pub fn read_value(&mut self) -> RedrawDecodeResult<Value> {
    self.ensure_remaining()?;
    let value = self.reader.read_value()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn read_raw_value(&mut self) -> RedrawDecodeResult<RawMsgpack<'de>> {
    self.ensure_remaining()?;
    let value = self.reader.read_raw_value()?;
    self.remaining -= 1;
    Ok(value)
  }

  pub fn skip_next(&mut self) -> RedrawDecodeResult<()> {
    self.ensure_remaining()?;
    self.reader.skip_value()?;
    self.remaining -= 1;
    Ok(())
  }

  pub fn skip_remaining(&mut self) -> RedrawDecodeResult<()> {
    while self.remaining > 0 {
      self.skip_next()?;
    }

    Ok(())
  }

  fn take_remaining(&mut self) -> Self {
    let remaining = self.remaining;
    self.remaining = 0;
    Self {
      reader: self.reader.clone(),
      remaining,
    }
  }

  fn ensure_remaining(&self) -> RedrawDecodeResult<()> {
    if self.remaining == 0 {
      return Err(RedrawDecodeError::Incomplete);
    }

    Ok(())
  }
}

/// A borrowed reader over msgpack map entries.
#[derive(Debug, Clone)]
pub struct MapReader<'de> {
  reader: MsgpackReader<'de>,
  remaining: u32,
}

impl<'de> MapReader<'de> {
  #[must_use]
  pub fn remaining(&self) -> u32 {
    self.remaining
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.remaining == 0
  }

  pub fn read_raw_pair(
    &mut self,
  ) -> RedrawDecodeResult<(RawMsgpack<'de>, RawMsgpack<'de>)> {
    self.ensure_remaining()?;
    let key = self.reader.read_raw_value()?;
    let value = self.reader.read_raw_value()?;
    self.remaining -= 1;
    Ok((key, value))
  }

  pub fn read_value_pair(&mut self) -> RedrawDecodeResult<(Value, Value)> {
    self.ensure_remaining()?;
    let key = self.reader.read_value()?;
    let value = self.reader.read_value()?;
    self.remaining -= 1;
    Ok((key, value))
  }

  pub fn skip_next(&mut self) -> RedrawDecodeResult<()> {
    self.ensure_remaining()?;
    self.reader.skip_value()?;
    self.reader.skip_value()?;
    self.remaining -= 1;
    Ok(())
  }

  pub fn skip_remaining(&mut self) -> RedrawDecodeResult<()> {
    while self.remaining > 0 {
      self.skip_next()?;
    }

    Ok(())
  }

  fn ensure_remaining(&self) -> RedrawDecodeResult<()> {
    if self.remaining == 0 {
      return Err(RedrawDecodeError::Incomplete);
    }

    Ok(())
  }
}

#[derive(Debug, Clone)]
struct MsgpackReader<'de> {
  input: &'de [u8],
  position: usize,
}

impl<'de> MsgpackReader<'de> {
  fn new(input: &'de [u8]) -> Self {
    Self { input, position: 0 }
  }

  fn remaining_slice(&self) -> &'de [u8] {
    &self.input[self.position..]
  }

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
    match decode::read_str_from_slice(self.remaining_slice()) {
      Ok((value, tail)) => {
        self.position = self.input.len() - tail.len();
        Ok(value)
      }
      Err(err) => Err(err.into()),
    }
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

  fn read_u64(&mut self) -> RedrawDecodeResult<u64> {
    Ok(self.read_rmp(decode::read_int::<u64, _>)?)
  }

  fn read_i64(&mut self) -> RedrawDecodeResult<i64> {
    Ok(self.read_rmp(decode::read_int::<i64, _>)?)
  }

  fn read_bool(&mut self) -> RedrawDecodeResult<bool> {
    Ok(self.read_rmp(decode::read_bool)?)
  }

  fn read_f32(&mut self) -> RedrawDecodeResult<f32> {
    Ok(self.read_rmp(decode::read_f32)?)
  }

  fn read_f64(&mut self) -> RedrawDecodeResult<f64> {
    Ok(self.read_rmp(decode::read_f64)?)
  }

  fn read_array_len(&mut self) -> RedrawDecodeResult<u32> {
    Ok(self.read_rmp(decode::read_array_len)?)
  }

  fn read_array_reader(&mut self) -> RedrawDecodeResult<ArrayReader<'de>> {
    self.read_array_len().map(|remaining| ArrayReader {
      reader: self.clone(),
      remaining,
    })
  }

  fn read_map_reader(&mut self) -> RedrawDecodeResult<MapReader<'de>> {
    let remaining = self.read_rmp(decode::read_map_len)?;
    Ok(MapReader {
      reader: self.clone(),
      remaining,
    })
  }

  fn read_value(&mut self) -> RedrawDecodeResult<Value> {
    let mut cursor = Cursor::new(&self.input[self.position..]);
    let value = read_value(&mut cursor)?;
    self.position += cursor.position() as usize;
    Ok(value)
  }

  fn read_raw_value(&mut self) -> RedrawDecodeResult<RawMsgpack<'de>> {
    let start = self.position;
    self.skip_value()?;
    Ok(RawMsgpack {
      bytes: &self.input[start..self.position],
    })
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

  fn skip_value(&mut self) -> RedrawDecodeResult<()> {
    // Redraw payloads come from the local Neovim process and are treated as
    // trusted input, so this skip reader intentionally does not enforce a
    // nesting depth limit.
    match self.read_rmp(decode::read_marker)? {
      Marker::FixPos(_)
      | Marker::FixNeg(_)
      | Marker::Null
      | Marker::False
      | Marker::True => Ok(()),
      Marker::FixMap(len) => self.skip_map_values(u32::from(len)),
      Marker::FixArray(len) => self.skip_values(u32::from(len)),
      Marker::FixStr(len) => self.skip_bytes(usize::from(len)),
      Marker::Bin8 => {
        let len = self.read_data_u8()? as usize;
        self.skip_bytes(len)
      }
      Marker::Bin16 => {
        let len = self.read_data_u16()? as usize;
        self.skip_bytes(len)
      }
      Marker::Bin32 => {
        let len = self.read_data_u32()? as usize;
        self.skip_bytes(len)
      }
      Marker::Ext8 => {
        let len = self.read_data_u8()? as usize;
        self.skip_ext_payload(len)
      }
      Marker::Ext16 => {
        let len = self.read_data_u16()? as usize;
        self.skip_ext_payload(len)
      }
      Marker::Ext32 => {
        let len = self.read_data_u32()? as usize;
        self.skip_ext_payload(len)
      }
      Marker::F32 => self.skip_bytes(size_of::<f32>()),
      Marker::F64 => self.skip_bytes(size_of::<f64>()),
      Marker::U8 | Marker::I8 => self.skip_bytes(size_of::<u8>()),
      Marker::U16 | Marker::I16 => self.skip_bytes(size_of::<u16>()),
      Marker::U32 | Marker::I32 => self.skip_bytes(size_of::<u32>()),
      Marker::U64 | Marker::I64 => self.skip_bytes(size_of::<u64>()),
      Marker::FixExt1 => self.skip_ext_payload(1),
      Marker::FixExt2 => self.skip_ext_payload(2),
      Marker::FixExt4 => self.skip_ext_payload(4),
      Marker::FixExt8 => self.skip_ext_payload(8),
      Marker::FixExt16 => self.skip_ext_payload(16),
      Marker::Str8 => {
        let len = self.read_data_u8()? as usize;
        self.skip_bytes(len)
      }
      Marker::Str16 => {
        let len = self.read_data_u16()? as usize;
        self.skip_bytes(len)
      }
      Marker::Str32 => {
        let len = self.read_data_u32()? as usize;
        self.skip_bytes(len)
      }
      Marker::Array16 => {
        let len = self.read_data_u16()?;
        self.skip_values(u32::from(len))
      }
      Marker::Array32 => {
        let len = self.read_data_u32()?;
        self.skip_values(len)
      }
      Marker::Map16 => {
        let len = self.read_data_u16()?;
        self.skip_map_values(u32::from(len))
      }
      Marker::Map32 => {
        let len = self.read_data_u32()?;
        self.skip_map_values(len)
      }
      Marker::Reserved => Err(RedrawDecodeError::Invalid(
        "reserved msgpack marker".to_owned(),
      )),
    }
  }

  fn skip_values(&mut self, count: u32) -> RedrawDecodeResult<()> {
    for _ in 0..count {
      self.skip_value()?;
    }

    Ok(())
  }

  fn skip_map_values(&mut self, len: u32) -> RedrawDecodeResult<()> {
    let count = len.checked_mul(2).ok_or_else(|| {
      RedrawDecodeError::Invalid("msgpack map length is too large".to_owned())
    })?;

    self.skip_values(count)
  }

  fn read_data_u8(&mut self) -> RedrawDecodeResult<u8> {
    Ok(self.read_rmp(RmpRead::read_data_u8)?)
  }

  fn read_data_u16(&mut self) -> RedrawDecodeResult<u16> {
    Ok(self.read_rmp(RmpRead::read_data_u16)?)
  }

  fn read_data_u32(&mut self) -> RedrawDecodeResult<u32> {
    Ok(self.read_rmp(RmpRead::read_data_u32)?)
  }

  fn read_data_u64(&mut self) -> RedrawDecodeResult<u64> {
    Ok(self.read_rmp(RmpRead::read_data_u64)?)
  }

  fn read_data_i8(&mut self) -> RedrawDecodeResult<i8> {
    Ok(self.read_rmp(RmpRead::read_data_i8)?)
  }

  fn read_data_i16(&mut self) -> RedrawDecodeResult<i16> {
    Ok(self.read_rmp(RmpRead::read_data_i16)?)
  }

  fn read_data_i32(&mut self) -> RedrawDecodeResult<i32> {
    Ok(self.read_rmp(RmpRead::read_data_i32)?)
  }

  fn read_data_i64(&mut self) -> RedrawDecodeResult<i64> {
    Ok(self.read_rmp(RmpRead::read_data_i64)?)
  }

  fn read_data_f32(&mut self) -> RedrawDecodeResult<f32> {
    Ok(self.read_rmp(RmpRead::read_data_f32)?)
  }

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

  fn skip_ext_payload(&mut self, data_len: usize) -> RedrawDecodeResult<()> {
    self.skip_bytes(1 + data_len)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use rmpv::encode::write_value;

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
  fn assert_reader_error_kind<T>(
    result: RedrawDecodeResult<T>,
    kind: ErrorKind,
  ) {
    let err = result.err().expect("expected reader error");
    let err = DecodeError::from(err);
    assert!(
      matches!(err, DecodeError::ReaderError(ref err) if err.kind() == kind)
    );
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

  fn len16_payload(
    marker: Marker,
    len: u16,
    includes_ext_type: bool,
  ) -> Vec<u8> {
    let mut bytes = vec![marker.to_u8()];
    push_u16(&mut bytes, len);
    if includes_ext_type {
      bytes.push(0);
    }
    bytes.extend(std::iter::repeat_n(1, len as usize));
    bytes
  }

  fn len32_payload(
    marker: Marker,
    len: u32,
    includes_ext_type: bool,
  ) -> Vec<u8> {
    let mut bytes = vec![marker.to_u8()];
    push_u32(&mut bytes, len);
    if includes_ext_type {
      bytes.push(0);
    }
    bytes.extend(std::iter::repeat_n(1, len as usize));
    bytes
  }

  #[test]
  fn is_redraw_method_accepts_redraw_notification() {
    let bytes =
      redraw_notification(vec![Value::from(vec![Value::from("flush")])]);

    assert!(is_redraw_method(&bytes).unwrap());
  }

  #[test]
  fn is_redraw_method_checks_rpc_method_and_params_header() {
    assert_eq!(
      is_redraw_method(&encode_value(Value::from("redraw"))).unwrap(),
      false
    );
    assert!(!is_redraw_method(&rpc_message(Vec::new())).unwrap());
    assert_eq!(
      is_redraw_method(&rpc_message(vec![Value::from(2)])).unwrap(),
      false
    );

    let request = rpc_message(vec![
      Value::from(0),
      Value::from(7),
      Value::from("redraw"),
      Value::from(Vec::<Value>::new()),
    ]);
    assert!(!is_redraw_method(&request).unwrap());

    let response = rpc_message(vec![
      Value::from(1),
      Value::from(7),
      Value::Nil,
      Value::from(true),
    ]);
    assert!(!is_redraw_method(&response).unwrap());

    let non_redraw = rpc_message(vec![
      Value::from(2),
      Value::from("not-redraw"),
      Value::from(Vec::<Value>::new()),
    ]);
    assert!(!is_redraw_method(&non_redraw).unwrap());

    let method_only = rpc_message(vec![Value::from(2), Value::from("redraw")]);
    assert!(!is_redraw_method(&method_only).unwrap());

    let non_array_params = rpc_message(vec![
      Value::from(2),
      Value::from("redraw"),
      Value::from("not-array"),
    ]);
    assert!(!is_redraw_method(&non_array_params).unwrap());
  }

  #[test]
  fn is_redraw_method_reports_incomplete_prefix() {
    assert_incomplete(is_redraw_method(&[]));

    let missing_params_header = [
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
    assert_incomplete(is_redraw_method(&missing_params_header));
  }

  #[test]
  fn is_redraw_method_does_not_read_payload_tail() {
    let bytes = [
      Marker::FixArray(4).to_u8(),
      2,
      Marker::FixStr(6).to_u8(),
      b'r',
      b'e',
      b'd',
      b'r',
      b'a',
      b'w',
      Marker::FixArray(0).to_u8(),
      Marker::FixStr(2).to_u8(),
      b'a',
    ];

    assert!(is_redraw_method(&bytes).unwrap());
  }

  #[test]
  fn redraw_frame_probe_reports_complete_incomplete_and_not_redraw() {
    let redraw =
      redraw_notification(vec![Value::from(vec![Value::from("flush")])]);
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

    let frame = RedrawFrame::probe(&redraw).unwrap().expect("redraw frame");
    assert_eq!(frame.consumed(), redraw.len());
    assert_eq!(frame.as_bytes(), redraw.as_slice());
    assert!(matches!(
      RedrawFrame::probe(&incomplete_redraw_prefix),
      Err(RedrawDecodeError::Incomplete)
    ));
    assert!(RedrawFrame::probe(&request).unwrap().is_none());
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
    let frame = RedrawFrame::probe(&redraw).unwrap().expect("redraw frame");
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
  fn is_redraw_method_reports_malformed_method_payloads() {
    let invalid_utf8_method = vec![
      Marker::FixArray(3).to_u8(),
      2,
      Marker::Str8.to_u8(),
      1,
      0xff,
      Marker::FixArray(0).to_u8(),
    ];
    assert_reader_error_kind(
      is_redraw_method(&invalid_utf8_method),
      ErrorKind::InvalidData,
    );

    let incomplete_method = vec![
      Marker::FixArray(3).to_u8(),
      2,
      Marker::Str8.to_u8(),
      2,
      b'a',
    ];
    assert_incomplete(is_redraw_method(&incomplete_method));

    let incomplete_first_item =
      vec![Marker::FixArray(3).to_u8(), Marker::U16.to_u8(), 1];
    assert_incomplete(is_redraw_method(&incomplete_first_item));
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
  fn redraw_batch_exposes_raw_payloads() {
    let bytes = redraw_notification(vec![Value::from(vec![
      Value::from("grid_line"),
      Value::from(vec![Value::from(1), Value::from("cell")]),
    ])]);
    let mut redraw = read_redraw_notification(&bytes);
    let mut payloads = Vec::new();

    redraw
      .for_each_batch(|batch| {
        while !batch.args.is_empty() {
          let payload = batch.args.read_raw_value()?;
          let mut input = payload.as_bytes();
          payloads.push(read_value(&mut input)?);
          assert!(input.is_empty());
        }
        Ok(true)
      })
      .unwrap();

    assert_eq!(
      payloads,
      vec![Value::from(vec![Value::from(1), Value::from("cell")])]
    );
  }

  #[test]
  fn raw_msgpack_reads_scalar_values() {
    let bytes = redraw_notification(vec![Value::from(vec![
      Value::from("values"),
      Value::from("text"),
      Value::from(-1),
      Value::from(2),
      Value::from(true),
    ])]);
    let mut redraw = read_redraw_notification(&bytes);

    redraw
      .for_each_batch(|batch| {
        assert_eq!(batch.name, "values");
        assert_eq!(batch.args.read_raw_value()?.as_str()?, "text");
        assert_eq!(batch.args.read_raw_value()?.as_i64()?, -1);
        assert_eq!(batch.args.read_raw_value()?.as_u64()?, 2);
        assert!(batch.args.read_raw_value()?.as_bool()?);
        Ok(true)
      })
      .unwrap();
  }

  #[test]
  fn readers_read_float_values() {
    let f32_value = 1.25_f32;
    let f64_value = -2.5_f64;
    let mut bytes = vec![Marker::FixArray(2).to_u8()];
    bytes.extend_from_slice(&encoded_f32(f32_value));
    bytes.extend_from_slice(&encoded_f64(f64_value));

    let mut reader = MsgpackReader::new(&bytes);
    let mut array = reader.read_array_reader().unwrap();

    let raw_f32 = array.read_raw_value().unwrap();
    let raw_f64 = array.read_raw_value().unwrap();
    assert_eq!(raw_f32.as_f32().unwrap(), f32_value);
    assert_eq!(raw_f64.as_f64().unwrap(), f64_value);

    let mut reader = MsgpackReader::new(&bytes);
    let mut array = reader.read_array_reader().unwrap();
    assert_eq!(array.read_f32().unwrap(), f32_value);
    assert_eq!(array.read_f64().unwrap(), f64_value);
  }

  #[test]
  fn array_reader_new_reads_array_payload() {
    let bytes =
      encode_value(Value::from(vec![Value::from(24), Value::from("text")]));
    let mut array = ArrayReader::new(&bytes).unwrap();

    assert_eq!(array.remaining(), 2);
    assert_eq!(array.read_u64().unwrap(), 24);
    assert_eq!(array.read_str().unwrap(), "text");
    assert!(array.is_empty());
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
  fn array_reader_reads_maps_as_raw_pairs() {
    let bytes = redraw_notification(vec![Value::from(vec![
      Value::from("option_set"),
      Value::from(vec![Value::from(vec![(Value::from("k"), Value::from(1))])]),
    ])]);
    let mut redraw = read_redraw_notification(&bytes);

    redraw
      .for_each_batch(|batch| {
        while !batch.args.is_empty() {
          batch.args.read_array(|args| {
            args.read_map(|map| {
              let (key, value) = map.read_value_pair()?;
              assert_eq!(key, Value::from("k"));
              assert_eq!(value, Value::from(1));
              Ok(())
            })
          })?;
        }
        Ok(true)
      })
      .unwrap();
  }

  #[test]
  fn array_reader_reads_values_and_reports_boundaries() {
    let bytes = encode_value(Value::from(vec![Value::from("value")]));
    let mut reader = MsgpackReader::new(&bytes);
    let mut array = reader.read_array_reader().unwrap();

    assert_eq!(array.read_value().unwrap(), Value::from("value"));
    assert_reader_error_kind(array.read_value(), ErrorKind::UnexpectedEof);
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
    assert_reader_error_kind(
      array.read_array(|_| Ok(())),
      ErrorKind::InvalidData,
    );
    assert_reader_error_kind(
      array.read_map(|_| Ok(())),
      ErrorKind::InvalidData,
    );
  }

  #[test]
  fn map_reader_reads_raw_pairs_and_skips_entries() {
    let bytes = encode_value(Value::from(vec![
      (Value::from("k1"), Value::from(1)),
      (Value::from("k2"), Value::from(2)),
    ]));
    let mut reader = MsgpackReader::new(&bytes);
    let mut map = reader.read_map_reader().unwrap();

    assert_eq!(map.remaining(), 2);
    assert!(!map.is_empty());

    let (key, value) = map.read_raw_pair().unwrap();
    let mut key_bytes = key.as_bytes();
    let mut value_bytes = value.as_bytes();
    assert_eq!(read_value(&mut key_bytes).unwrap(), Value::from("k1"));
    assert_eq!(read_value(&mut value_bytes).unwrap(), Value::from(1));

    map.skip_next().unwrap();
    assert!(map.is_empty());
    assert_reader_error_kind(map.skip_next(), ErrorKind::UnexpectedEof);
  }

  #[test]
  fn map_reader_skips_remaining_entries() {
    let bytes =
      encode_value(Value::from(vec![(Value::from("k"), Value::from("v"))]));
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

    let mut map32_too_large = vec![Marker::Map32.to_u8()];
    push_u32(&mut map32_too_large, u32::MAX);
    let mut reader = MsgpackReader::new(&map32_too_large);
    assert_reader_error_kind(reader.skip_value(), ErrorKind::InvalidData);
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
    assert!(reader.read_value().is_err());

    let mut reader = MsgpackReader::new(&[]);
    assert_reader_error_kind(
      reader.read_str().map(|_| ()),
      ErrorKind::UnexpectedEof,
    );
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
