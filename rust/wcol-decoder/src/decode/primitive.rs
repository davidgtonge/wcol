
use std::convert::TryInto;

use crate::constants::{
    TYPE_I8, TYPE_I16, TYPE_I32, TYPE_U8, TYPE_U16, TYPE_U32,
};

use super::E_DECODE;

fn decode_le<T, const N: usize>(
    bytes: &[u8],
    f: impl Fn([u8; N]) -> T,
) -> Result<Vec<T>, i32> {
    if bytes.len() % N != 0 {
        return Err(E_DECODE);
    }

    Ok(bytes
        .chunks_exact(N)
        .map(|chunk| f(chunk.try_into().unwrap()))
        .collect())
}

pub(crate) fn decode_u8(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

pub(crate) fn decode_i8(bytes: &[u8]) -> Result<Vec<i8>, i32> {
    decode_le::<i8, 1>(bytes, |b| b[0] as i8)
}

pub(crate) fn decode_u16(bytes: &[u8]) -> Result<Vec<u16>, i32> {
    decode_le(bytes, u16::from_le_bytes)
}

pub(crate) fn decode_u32(bytes: &[u8]) -> Result<Vec<u32>, i32> {
    decode_le(bytes, u32::from_le_bytes)
}

pub(crate) fn decode_i16(bytes: &[u8]) -> Result<Vec<i16>, i32> {
    decode_le(bytes, i16::from_le_bytes)
}

pub(crate) fn decode_i32(bytes: &[u8]) -> Result<Vec<i32>, i32> {
    decode_le(bytes, i32::from_le_bytes)
}

pub(crate) fn decode_i64(bytes: &[u8]) -> Result<Vec<i64>, i32> {
    decode_le(bytes, i64::from_le_bytes)
}

pub(crate) fn decode_f32(bytes: &[u8]) -> Result<Vec<f64>, i32> {
    decode_le(bytes, |b| f32::from_le_bytes(b) as f64)
}

pub(crate) fn decode_f64(bytes: &[u8]) -> Result<Vec<f64>, i32> {
    decode_le(bytes, f64::from_le_bytes)
}

fn widen_vec<T, U, F>(values: Vec<T>, map: F) -> Vec<U>
where
    F: Fn(T) -> U,
{
    let mut out = Vec::with_capacity(values.len());
    out.extend(values.into_iter().map(map));
    out
}

pub(crate) fn decode_index_ids_as_u32(ty: u8, raw: &[u8]) -> Result<Vec<u32>, i32> {
    match ty {
        TYPE_U8 => Ok(widen_vec(decode_u8(raw), |v| v as u32)),
        TYPE_U16 => Ok(widen_vec(decode_u16(raw)?, |v| v as u32)),
        TYPE_U32 => decode_u32(raw),
        TYPE_I8 => Ok(widen_vec(decode_i8(raw)?, |v| v as u32)),
        TYPE_I16 => Ok(widen_vec(decode_i16(raw)?, |v| v as u32)),
        TYPE_I32 => Ok(widen_vec(decode_i32(raw)?, |v| v as u32)),
        _ => Err(E_DECODE),
    }
}
