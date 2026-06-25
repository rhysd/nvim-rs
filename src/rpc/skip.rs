//! Shared msgpack value skipper.

use std::{
    io::{self, ErrorKind},
    mem::size_of,
};

use rmp::Marker;

use crate::error::DecodeError;

#[inline]
pub(super) fn skip_value(input: &[u8]) -> Result<usize, Box<DecodeError>> {
    skip_value_at(input, 0)
}

#[inline(never)]
fn skip_value_at(input: &[u8], offset: usize) -> Result<usize, Box<DecodeError>> {
    // Neovim is local trusted input here. Match the redraw fast path and avoid
    // a nesting depth limit while still preserving malformed/truncated errors.
    let marker = Marker::from_u8(read_u8(input, offset)?);
    let offset = offset + size_of::<u8>();
    match marker {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::False | Marker::True => {
            Ok(offset)
        }
        Marker::FixMap(len) => skip_values(input, offset, len as u32 * 2),
        Marker::FixArray(len) => skip_values(input, offset, len as u32),
        Marker::FixStr(len) => skip_bytes(input, offset, len as u32),
        Marker::Bin8 => {
            let len = read_u8(input, offset)? as u32;
            skip_bytes(input, offset + size_of::<u8>(), len)
        }
        Marker::Bin16 => {
            let len = read_u16(input, offset)? as u32;
            skip_bytes(input, offset + size_of::<u16>(), len)
        }
        Marker::Bin32 => {
            let len = read_u32(input, offset)?;
            skip_bytes(input, offset + size_of::<u32>(), len)
        }
        Marker::Ext8 => {
            let len = read_u8(input, offset)? as u32;
            skip_ext_payload(input, offset + size_of::<u8>(), len)
        }
        Marker::Ext16 => {
            let len = read_u16(input, offset)? as u32;
            skip_ext_payload(input, offset + size_of::<u16>(), len)
        }
        Marker::Ext32 => {
            let len = read_u32(input, offset)?;
            skip_ext_payload(input, offset + size_of::<u32>(), len)
        }
        Marker::F32 => skip_bytes(input, offset, size_of::<f32>() as u32),
        Marker::F64 => skip_bytes(input, offset, size_of::<f64>() as u32),
        Marker::U8 | Marker::I8 => skip_bytes(input, offset, size_of::<u8>() as u32),
        Marker::U16 | Marker::I16 => skip_bytes(input, offset, size_of::<u16>() as u32),
        Marker::U32 | Marker::I32 => skip_bytes(input, offset, size_of::<u32>() as u32),
        Marker::U64 | Marker::I64 => skip_bytes(input, offset, size_of::<u64>() as u32),
        Marker::FixExt1 => skip_ext_payload(input, offset, 1),
        Marker::FixExt2 => skip_ext_payload(input, offset, 2),
        Marker::FixExt4 => skip_ext_payload(input, offset, 4),
        Marker::FixExt8 => skip_ext_payload(input, offset, 8),
        Marker::FixExt16 => skip_ext_payload(input, offset, 16),
        Marker::Str8 => {
            let len = read_u8(input, offset)? as u32;
            skip_bytes(input, offset + size_of::<u8>(), len)
        }
        Marker::Str16 => {
            let len = read_u16(input, offset)? as u32;
            skip_bytes(input, offset + size_of::<u16>(), len)
        }
        Marker::Str32 => {
            let len = read_u32(input, offset)?;
            skip_bytes(input, offset + size_of::<u32>(), len)
        }
        Marker::Array16 => {
            let len = read_u16(input, offset)? as u32;
            skip_values(input, offset + size_of::<u16>(), len)
        }
        Marker::Array32 => {
            let len = read_u32(input, offset)?;
            skip_values(input, offset + size_of::<u32>(), len)
        }
        Marker::Map16 => {
            let len = read_u16(input, offset)? as u32;
            skip_values(input, offset + size_of::<u16>(), len * 2)
        }
        Marker::Map32 => {
            let len = read_u32(input, offset)?;
            skip_values(input, offset + size_of::<u32>(), len * 2)
        }
        Marker::Reserved => Err(rmpv::decode::Error::InvalidDataRead(io::Error::new(
            ErrorKind::InvalidData,
            "reserved msgpack marker",
        ))
        .into()),
    }
}

#[inline]
fn skip_values(input: &[u8], mut offset: usize, count: u32) -> Result<usize, Box<DecodeError>> {
    for _ in 0..count {
        offset = skip_value_at(input, offset)?;
    }
    Ok(offset)
}

#[inline]
fn skip_ext_payload(input: &[u8], offset: usize, data_len: u32) -> Result<usize, Box<DecodeError>> {
    skip_bytes(input, offset, data_len + 1)
}

#[inline]
fn skip_bytes(input: &[u8], offset: usize, len: u32) -> Result<usize, Box<DecodeError>> {
    let end = offset + len as usize;
    if end > input.len() {
        return Err(Box::new(DecodeError::BufferError(
            rmpv::decode::Error::InvalidDataRead(io::Error::new(
                ErrorKind::UnexpectedEof,
                "incomplete msgpack payload",
            )),
        )));
    }
    Ok(end)
}

#[inline]
fn read_u8(input: &[u8], offset: usize) -> Result<u8, Box<DecodeError>> {
    skip_bytes(input, offset, size_of::<u8>() as u32)?;
    Ok(input[offset])
}

#[inline]
fn read_u16(input: &[u8], offset: usize) -> Result<u16, Box<DecodeError>> {
    let end = skip_bytes(input, offset, size_of::<u16>() as u32)?;
    Ok(u16::from_be_bytes(input[offset..end].try_into().unwrap()))
}

#[inline]
fn read_u32(input: &[u8], offset: usize) -> Result<u32, Box<DecodeError>> {
    let end = skip_bytes(input, offset, size_of::<u32>() as u32)?;
    Ok(u32::from_be_bytes(input[offset..end].try_into().unwrap()))
}
