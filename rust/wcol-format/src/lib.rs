//! Shared `.wcol` format constants for encoder and decoder.

/// Only supported on-disk format version.
pub const WCOL_VERSION: u16 = 7;

pub const ROWS_PER_CHUNK: usize = 65504;

pub const HEADER_BYTES: usize = 96;
pub const HEADER_INFO_BYTES: usize = 92;
pub const TOC_ENTRY_BYTES: usize = 8;
pub const INDEX_ENTRY_BYTES: usize = 80;

pub const NULL_SENTINEL: u32 = 0xffff_ffff;
pub const NULL_SENTINEL_U64: u64 = 0xffff_ffff_ffff_ffff;

pub const FLAG_NULLABLE: u8 = 1;
pub const FLAG_DICT: u8 = 2;

pub const ENCODING_NONE: u8 = 0;
pub const ENCODING_NUM_DICT: u8 = 1;

pub const TYPE_U8: u8 = 0;
pub const TYPE_U16: u8 = 1;
pub const TYPE_U32: u8 = 2;
pub const TYPE_I32: u8 = 3;
pub const TYPE_I64: u8 = 4;
pub const TYPE_F32: u8 = 5;
pub const TYPE_F64: u8 = 6;
pub const TYPE_BOOL: u8 = 7;
pub const TYPE_I16: u8 = 8;
pub const TYPE_I8: u8 = 9;
pub const TYPE_STRING: u8 = 10;

pub const EMPTY_MODE_ALL_ZERO: u8 = 0;
pub const EMPTY_MODE_ALL_ONE: u8 = 1;
pub const EMPTY_MODE_MIXED: u8 = 2;

pub const STRING_BLOOM_BLOCK_ROWS: usize = 64;
pub const STRING_BLOOM_BITS: usize = 1024;
pub const STRING_BLOOM_NGRAM: usize = 4;
