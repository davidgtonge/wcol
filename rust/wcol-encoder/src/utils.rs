use anyhow::{Context, Result};

use crate::constants::{
    FLAG_DICT, FLAG_NULLABLE, TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16, TYPE_I32, TYPE_I64, TYPE_I8,
    TYPE_STRING, TYPE_U16, TYPE_U32, TYPE_U8,
};
use crate::types::{ChunkPages, ColumnSpec, ColumnTotals};

pub(crate) fn decode_binary_value<'a>(value: &'a [u8], col_name: &str) -> Result<&'a str> {
    std::str::from_utf8(value)
        .with_context(|| format!("invalid UTF-8 in binary column {}", col_name))
}

pub(crate) fn scaled_f64(value: f64, scale: i32) -> f64 {
    (value * scale as f64).round()
}

pub(crate) fn set_bit(bitmap: &mut [u8], idx: usize) {
    let byte = idx >> 3;
    let bit = idx & 7;
    bitmap[byte] |= 1 << bit;
}

pub(crate) fn is_valid(bitmap: &[u8], idx: usize) -> bool {
    let byte = bitmap[idx >> 3];
    let bit = idx & 7;
    (byte & (1 << bit)) != 0
}

pub(crate) fn print_schema(columns: &[ColumnSpec]) {
    println!("Schema:");
    for col in columns {
        let mut extras = String::new();
        if (col.flags & FLAG_DICT) != 0 {
            extras = format!(
                " dict_width={} dict_len={}",
                col.dict_index_width,
                col.dict_values.len()
            );
        }
        println!(
            "  {} {} logical={} physical={} scale={} flags={}{}",
            col.id,
            col.name,
            type_name(col.logical_type),
            type_name(col.physical_type),
            col.scale,
            format_flags(col.flags),
            extras
        );
    }
}

pub(crate) fn print_stats(columns: &[ColumnSpec], chunks: &[ChunkPages]) {
    let mut totals = vec![
        ColumnTotals {
            raw_data: 0,
            comp_data: 0,
            raw_null: 0,
            comp_null: 0,
        };
        columns.len()
    ];

    for chunk in chunks {
        for (idx, page) in chunk.columns.iter().enumerate() {
            let total = &mut totals[idx];
            total.raw_data += page.data_raw_len as u64;
            total.comp_data += page.data_comp.len() as u64;
            total.raw_null += page.null_raw_len as u64;
            if let Some(null_comp) = &page.null_comp {
                total.comp_null += null_comp.len() as u64;
            }
        }
    }

    println!("Stats:");
    for (col, total) in columns.iter().zip(totals.iter()) {
        let dict_len = if (col.flags & FLAG_DICT) != 0 {
            col.dict_values.len()
        } else {
            0
        };
        let data_ratio = if total.raw_data > 0 {
            (total.comp_data as f64) / (total.raw_data as f64)
        } else {
            0.0
        };
        let null_ratio = if total.raw_null > 0 {
            Some((total.comp_null as f64) / (total.raw_null as f64))
        } else {
            None
        };
        if let Some(null_ratio) = null_ratio {
            println!(
                "  {} {} physical={} dict={} data={:.3} ({} / {}) null={:.3} ({} / {})",
                col.id,
                col.name,
                type_name(col.physical_type),
                dict_len,
                data_ratio,
                total.comp_data,
                total.raw_data,
                null_ratio,
                total.comp_null,
                total.raw_null
            );
        } else {
            println!(
                "  {} {} physical={} dict={} data={:.3} ({} / {}) null=none",
                col.id,
                col.name,
                type_name(col.physical_type),
                dict_len,
                data_ratio,
                total.comp_data,
                total.raw_data
            );
        }
    }
}

pub(crate) fn format_flags(flags: u8) -> String {
    if flags == 0 {
        return "none".to_string();
    }
    let mut parts = Vec::new();
    if (flags & FLAG_NULLABLE) != 0 {
        parts.push("nullable");
    }
    if (flags & FLAG_DICT) != 0 {
        parts.push("dict");
    }
    parts.join("|")
}

pub(crate) fn type_name(ty: u8) -> &'static str {
    match ty {
        TYPE_U8 => "u8",
        TYPE_U16 => "u16",
        TYPE_U32 => "u32",
        TYPE_I8 => "i8",
        TYPE_I16 => "i16",
        TYPE_I32 => "i32",
        TYPE_I64 => "i64",
        TYPE_F32 => "f32",
        TYPE_F64 => "f64",
        TYPE_BOOL => "bool",
        TYPE_STRING => "string",
        _ => "unknown",
    }
}
