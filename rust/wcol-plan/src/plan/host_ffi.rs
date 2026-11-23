//! Coordinator FFI. SQL enters here, not in the engine.
//!
//! # Safety
//!
//! Every export shares one contract: each `(ptr, len)` pair names `len` valid bytes in Wasm
//! memory (writable for output buffers), and the host is single-threaded and never calls in
//! re-entrantly.

// The shared contract above covers each `unsafe extern "C"` export.
#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::slice;

use crate::error::{clear_last_error, set_last_error, WcolError};
use crate::ir::CatalogColumn;
use crate::plan::columns_for_sql;
use crate::program::stream_program::PlanCursor;

thread_local! {
    static PLAN_OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static RESULT_OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Catalog bytes (`encode_catalog`) + SQL → needed column ids in `out` (`u32` count, then ids).
/// Returns bytes written, or `0` on error.
#[no_mangle]
pub unsafe extern "C" fn wcol_columns_for_sql(
    catalog_ptr: *const u8,
    catalog_len: u32,
    sql_ptr: *const u8,
    sql_len: u32,
    out_ptr: *mut u8,
    out_cap: u32,
) -> u32 {
    if catalog_ptr.is_null()
        || catalog_len == 0
        || sql_ptr.is_null()
        || sql_len == 0
        || out_ptr.is_null()
        || out_cap < 4
    {
        return ffi_fail("invalid columns_for_sql buffer");
    }
    let catalog_bytes = slice::from_raw_parts(catalog_ptr, catalog_len as usize);
    let mut cursor = PlanCursor::new(catalog_bytes);
    let catalog: Vec<CatalogColumn> = match cursor.catalog() {
        Ok(c) => c,
        Err(e) => return ffi_fail_msg(format!("catalog: {e}")),
    };
    let sql_bytes = slice::from_raw_parts(sql_ptr, sql_len as usize);
    let sql = match std::str::from_utf8(sql_bytes) {
        Ok(s) => s,
        Err(_) => return ffi_fail("sql is not utf-8"),
    };
    let needed = match columns_for_sql(sql, &catalog) {
        Ok(n) => n,
        Err(e) => return ffi_fail_msg(e),
    };
    let out_len = 4 + needed.columns.len() * 4;
    if out_len > out_cap as usize {
        return ffi_fail("columns_for_sql output buffer too small");
    }
    let out = slice::from_raw_parts_mut(out_ptr, out_cap as usize);
    out[..4].copy_from_slice(&(needed.columns.len() as u32).to_le_bytes());
    for (i, id) in needed.columns.iter().enumerate() {
        let at = 4 + i * 4;
        out[at..at + 4].copy_from_slice(&id.to_le_bytes());
    }
    clear_last_error();
    out_len as u32
}

/// Plan a query from SQL and a Parquet footer (see [`plan_query_dispatch_wire`](crate::plan::plan_query_dispatch_wire)).
/// Returns the output length (read it at [`wcol_plan_output_ptr`]), or `0` with the last error.
#[no_mangle]
pub unsafe extern "C" fn wcol_plan_query(ptr: *const u8, len: u32) -> u32 {
    if ptr.is_null() || len == 0 {
        return ffi_fail("empty plan request");
    }
    match crate::plan::plan_query_dispatch_wire(slice::from_raw_parts(ptr, len as usize)) {
        Ok(out) => match u32::try_from(out.len()) {
            Ok(n) => {
                PLAN_OUTPUT.with(|cell| *cell.borrow_mut() = out);
                clear_last_error();
                n
            }
            Err(_) => ffi_fail("plan output exceeds 4 GiB"),
        },
        Err(err) => ffi_fail_msg(format!("plan query: {err}")),
    }
}

/// A `POST /jobs` body (UTF-8 JSON) → `{"kind":..,"sources":[..]}`: the objects whose footers
/// [`wcol_plan_job`] needs, in order. Output at [`wcol_plan_output_ptr`].
#[no_mangle]
pub unsafe extern "C" fn wcol_job_sources(ptr: *const u8, len: u32) -> u32 {
    if ptr.is_null() || len == 0 {
        return ffi_fail("empty job body");
    }
    let Ok(body) = std::str::from_utf8(slice::from_raw_parts(ptr, len as usize)) else {
        return ffi_fail("job body is not UTF-8");
    };
    match crate::plan::job_plan::job_sources(body) {
        Ok((request, sources)) => set_plan_output(
            crate::plan::job_plan::job_sources_json(&request, &sources).into_bytes(),
        ),
        Err(err) => ffi_fail_msg(format!("job: {err}")),
    }
}

/// Build a page-compact concat program from shard footers (preserve row groups).
#[no_mangle]
pub unsafe extern "C" fn wcol_concat_program(ptr: *const u8, len: u32) -> u32 {
    if ptr.is_null() || len == 0 {
        return ffi_fail("empty concat program request");
    }
    match crate::plan::job_plan::concat_program_wire(slice::from_raw_parts(ptr, len as usize)) {
        Ok(out) => set_plan_output(out),
        Err(err) => ffi_fail_msg(format!("concat program: {err}")),
    }
}

/// Plan a job (see [`plan_job_wire`](crate::plan::job_plan::plan_job_wire)). Output at
/// [`wcol_plan_output_ptr`].
#[no_mangle]
pub unsafe extern "C" fn wcol_plan_job(ptr: *const u8, len: u32) -> u32 {
    if ptr.is_null() || len == 0 {
        return ffi_fail("empty job plan request");
    }
    match crate::plan::job_plan::plan_job_wire(slice::from_raw_parts(ptr, len as usize)) {
        Ok(out) => set_plan_output(out),
        Err(err) => ffi_fail_msg(format!("plan job: {err}")),
    }
}

/// Plan a late row job's read-back (see
/// [`plan_late_read_wire`](crate::plan::job_plan::plan_late_read_wire)). Output at
/// [`wcol_plan_output_ptr`].
#[no_mangle]
pub unsafe extern "C" fn wcol_plan_late_read(ptr: *const u8, len: u32) -> u32 {
    if ptr.is_null() || len == 0 {
        return ffi_fail("empty late read request");
    }
    match crate::plan::job_plan::plan_late_read_wire(slice::from_raw_parts(ptr, len as usize)) {
        Ok(out) => set_plan_output(out),
        Err(err) => ffi_fail_msg(format!("plan late read: {err}")),
    }
}

fn set_plan_output(out: Vec<u8>) -> u32 {
    match u32::try_from(out.len()) {
        Ok(n) => {
            PLAN_OUTPUT.with(|cell| *cell.borrow_mut() = out);
            clear_last_error();
            n
        }
        Err(_) => ffi_fail("plan output exceeds 4 GiB"),
    }
}

/// Bytes of the last [`wcol_plan_query`] result; valid until the next call.
#[no_mangle]
pub extern "C" fn wcol_plan_output_ptr() -> *const u8 {
    PLAN_OUTPUT.with(|cell| cell.borrow().as_ptr())
}

/// Result JSON (`{"columns":[..],"rows":[[..],..]}`) at the client edge. `columns` is the
/// result-column trailer of the [`wcol_plan_query`] output. `data` is the root task's WCS1 row
/// frames (`kind` 0), or a footer-only answer's partial (`kind` 1). `row_cap` (`0`: none)
/// bounds the rows printed; a capped body carries `"truncated":true`.
/// Returns the byte length (read it at [`wcol_result_output_ptr`]), or `0` with the last error.
#[no_mangle]
pub unsafe extern "C" fn wcol_result_json(
    columns_ptr: *const u8,
    columns_len: u32,
    kind: u32,
    data_ptr: *const u8,
    data_len: u32,
    row_cap: u32,
) -> u32 {
    if columns_ptr.is_null() || columns_len == 0 || (data_ptr.is_null() && data_len > 0) {
        return ffi_fail("result json needs columns");
    }
    let columns = match crate::plan::result::decode_result_columns(slice::from_raw_parts(
        columns_ptr,
        columns_len as usize,
    )) {
        Ok(columns) => columns,
        Err(err) => return ffi_fail_msg(format!("result columns: {err:?}")),
    };
    let data: &[u8] = if data_len == 0 {
        &[]
    } else {
        slice::from_raw_parts(data_ptr, data_len as usize)
    };
    let json = match kind {
        0 => crate::plan::result::rows_json(&columns, data, row_cap),
        1 => crate::plan::result::partial_json(&columns, data),
        _ => return ffi_fail("result json kind"),
    };
    match json {
        Ok(json) => set_result_output(json),
        Err(err) => ffi_fail_msg(format!("result json: {err}")),
    }
}

/// Result JSON of a late row job: `pick_columns` and `pick` are the pick graph's result
/// columns and root row frames (the ids in result order), `columns` and `rows` the read-back
/// graph's. Output and errors as [`wcol_result_json`].
#[no_mangle]
pub unsafe extern "C" fn wcol_rows_by_id_json(
    pick_columns_ptr: *const u8,
    pick_columns_len: u32,
    pick_ptr: *const u8,
    pick_len: u32,
    columns_ptr: *const u8,
    columns_len: u32,
    rows_ptr: *const u8,
    rows_len: u32,
    row_cap: u32,
) -> u32 {
    let bytes = |ptr: *const u8, len: u32| -> &[u8] {
        if ptr.is_null() || len == 0 {
            &[]
        } else {
            slice::from_raw_parts(ptr, len as usize)
        }
    };
    let decode = |ptr, len| {
        crate::plan::result::decode_result_columns(bytes(ptr, len))
            .map_err(|err| format!("result columns: {err:?}"))
    };
    let json = decode(pick_columns_ptr, pick_columns_len).and_then(|pick_columns| {
        let [row_id] = pick_columns.as_slice() else {
            return Err("a pick prints one row-id column".to_string());
        };
        let order =
            crate::plan::result::i64_column_values(bytes(pick_ptr, pick_len), row_id.column)?;
        crate::plan::result::rows_by_id_json(
            &decode(columns_ptr, columns_len)?,
            row_id.column,
            &order,
            bytes(rows_ptr, rows_len),
            row_cap,
        )
    });
    match json {
        Ok(json) => set_result_output(json),
        Err(err) => ffi_fail_msg(format!("result json: {err}")),
    }
}

fn set_result_output(json: String) -> u32 {
    match u32::try_from(json.len()) {
        Ok(n) => {
            RESULT_OUTPUT.with(|cell| *cell.borrow_mut() = json.into_bytes());
            clear_last_error();
            n
        }
        Err(_) => ffi_fail("result json exceeds 4 GiB"),
    }
}

/// Bytes of the last [`wcol_result_json`] result; valid until the next call.
#[no_mangle]
pub extern "C" fn wcol_result_output_ptr() -> *const u8 {
    RESULT_OUTPUT.with(|cell| cell.borrow().as_ptr())
}

/// Function address kept alive by the cdylib. `*const ()` is not `Sync`.
/// The pointer is intentionally unread; the relocation is the point.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub struct FfiSymbol(*const ());
unsafe impl Sync for FfiSymbol {}
unsafe impl Send for FfiSymbol {}

/// C ABI exports. The `wcol` cdylib copies this into a `#[used]` static so the linker
/// keeps the symbols when this crate is only an rlib dependency.
pub const EXPORTED_FFI_LEN: usize = 10;

pub static EXPORTED_FFI: [FfiSymbol; EXPORTED_FFI_LEN] = [
    FfiSymbol(wcol_columns_for_sql as *const ()),
    FfiSymbol(wcol_plan_query as *const ()),
    FfiSymbol(wcol_plan_output_ptr as *const ()),
    FfiSymbol(wcol_result_json as *const ()),
    FfiSymbol(wcol_result_output_ptr as *const ()),
    FfiSymbol(wcol_job_sources as *const ()),
    FfiSymbol(wcol_plan_job as *const ()),
    FfiSymbol(wcol_concat_program as *const ()),
    FfiSymbol(wcol_plan_late_read as *const ()),
    FfiSymbol(wcol_rows_by_id_json as *const ()),
];

fn ffi_fail(msg: &'static str) -> u32 {
    set_last_error(WcolError::from(msg));
    0
}

fn ffi_fail_msg(msg: impl Into<String>) -> u32 {
    set_last_error(WcolError::msg(msg));
    0
}
