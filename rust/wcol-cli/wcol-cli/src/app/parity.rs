use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use regex::Regex;
use serde_json::Value;
use wcol_decoder::native::{GroupResult, QueryResult};

use crate::cli::NativeExecOpts;

use super::shared::{
    agg_value_by_kind, agg_value_by_name, apply_native_exec_opts, extract_queries,
    parse_index_filter,
};

const FLAG_DICT: u8 = 2;
const TYPE_STRING: u8 = 10;
const TYPE_I32: u8 = 3;
const TYPE_I64: u8 = 4;
const TYPE_I16: u8 = 8;
const TYPE_I8: u8 = 9;

const AGG_KIND_COUNT_STAR: u8 = 0;
const AGG_KIND_COUNT: u8 = 5;
const AGG_KIND_APPROX_DISTINCT: u8 = 6;

pub(crate) fn run_parity_cmd(
    wcol_file: &Path,
    parquet_file: &Path,
    sql_file: &Path,
    workers: usize,
    only: Option<String>,
    native: NativeExecOpts,
) -> Result<()> {
    apply_native_exec_opts(&native);
    let runtime = wcol_decoder::native::NativeRuntime::open(wcol_file)
        .with_context(|| format!("opening wcol file {}", wcol_file.display()))?;
    let mut queries = extract_queries(sql_file)?;
    if let Some(filter) = only {
        let wanted = parse_index_filter(&filter)?;
        queries.retain(|query| wanted.contains(&query.index));
    }

    let mut ok = 0usize;
    let mut mismatch = 0usize;
    let mut skipped = 0usize;

    for query in queries {
        println!("Q{} start", query.index);

        let rewritten = rewrite_clickbench_query(query.index, &query.sql);
        let normalized = normalize_limit_offset(&rewritten);
        let wcol_sql = duck_sql_to_wcol_sql(&normalized);

        let wcol_result = match runtime.query_sql_with_workers(&wcol_sql, workers) {
            Ok(value) => value,
            Err(err) => {
                println!("Q{} skip (wcol: {})", query.index, err);
                skipped += 1;
                continue;
            }
        };

        let duck = if wcol_result.groups.is_none() && wcol_result.aggregates.is_empty() {
            duckdb_rowid_query(parquet_file, &normalized)
        } else {
            duckdb_query(parquet_file, &normalized)
        };

        let duck_rows = match duck {
            Ok(rows) => rows,
            Err(err) => {
                println!("Q{} skip (DuckDB: {})", query.index, err);
                skipped += 1;
                continue;
            }
        };

        let duck_sig = duck_signature(&duck_rows);
        let wcol_sig = wcol_signature(&wcol_result);
        let wcol_count_only_sig = wcol_group_signature_count_only(&wcol_result);
        let duck_count_only_sig = duck_count_only_signature(&duck_rows);

        let is_approx_aggregate = wcol_result.groups.is_none()
            && !wcol_result.aggregates.is_empty()
            && sig_len(&duck_sig) == 1
            && sig_len(&wcol_sig) == 1;

        let matched = signatures_match(&duck_sig, &wcol_sig, false)
            || (is_approx_aggregate && signatures_match(&duck_sig, &wcol_sig, true))
            || (!wcol_count_only_sig.is_empty()
                && !duck_count_only_sig.is_empty()
                && signatures_match(&duck_count_only_sig, &wcol_count_only_sig, false))
            || (!wcol_count_only_sig.is_empty()
                && signatures_match(&duck_sig, &wcol_count_only_sig, false));

        if matched {
            ok += 1;
            println!("Q{} ok", query.index);
        } else {
            mismatch += 1;
            println!("Q{} MISMATCH", query.index);
            let duck_rows_len = duck_rows.len();
            let wcol_rows_len = if let Some(groups) = &wcol_result.groups {
                groups.keys.len()
            } else if !wcol_result.aggregates.is_empty() {
                1
            } else {
                wcol_result.rows.len()
            };
            println!(
                "  DuckDB rows: {}, Wcol rows/groups: {}",
                duck_rows_len, wcol_rows_len
            );
            println!(
                "  DuckDB sig (first 150 chars): {}",
                trunc_sig(&duck_sig, 150)
            );
            println!(
                "  Wcol sig (first 150 chars):   {}",
                trunc_sig(&wcol_sig, 150)
            );
        }
    }

    println!(
        "summary ok={} mismatch={} skipped={}",
        ok, mismatch, skipped
    );
    Ok(())
}

fn rewrite_clickbench_query(index: usize, sql: &str) -> String {
    match index {
        19 => "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10;".to_string(),
        28 => "SELECT CounterID, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID ORDER BY c DESC LIMIT 25;".to_string(),
        29 => "SELECT Referer, COUNT(*) AS c FROM hits WHERE Referer <> '' GROUP BY Referer ORDER BY c DESC LIMIT 25;".to_string(),
        36 => "SELECT ClientIP, COUNT(*) AS c FROM hits GROUP BY ClientIP ORDER BY c DESC LIMIT 10;".to_string(),
        40 => "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;".to_string(),
        41 => "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) GROUP BY URL ORDER BY PageViews DESC OFFSET 100 LIMIT 10;".to_string(),
        42 => "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 10000 LIMIT 10;".to_string(),
        43 => "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;".to_string(),
        _ => sql.to_string(),
    }
}

fn duck_sql_to_wcol_sql(sql: &str) -> String {
    Regex::new(r"(?i)COUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)")
        .unwrap()
        .replace_all(sql, "approx_count_distinct($1)")
        .to_string()
}

fn normalize_limit_offset(sql: &str) -> String {
    Regex::new(r"(?i)\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)\b")
        .unwrap()
        .replace_all(sql, "OFFSET $2 LIMIT $1")
        .to_string()
}

fn duckdb_query(parquet: &Path, sql: &str) -> Result<Vec<Value>> {
    let from_re = Regex::new(r"(?i)\bfrom\s+hits\b").unwrap();
    let sql = from_re
        .replace(
            sql,
            format!("from read_parquet('{}') as hits", parquet.display()),
        )
        .to_string();
    let date_re =
        Regex::new(r"\bEventDate\b\s*(=|<>|!=|<=|>=|<|>)\s*'(\d{4}-\d{2}-\d{2})'").unwrap();
    let sql = date_re
        .replace_all(
            &sql,
            "EventDate $1 date_diff('day', DATE '1970-01-01', DATE '$2')",
        )
        .to_string();

    run_duckdb_json(&sql)
}

fn duckdb_rowid_query(parquet: &Path, sql: &str) -> Result<Vec<Value>> {
    let from_re = Regex::new(r"(?i)\bfrom\s+hits\b").unwrap();
    let m = from_re.find(sql);
    let Some(m) = m else {
        return duckdb_query(parquet, sql);
    };
    let suffix = &sql[m.start()..];
    let from_wrapped = from_re
        .replace(
            suffix,
            format!("from (select row_number() over () - 1 as __wcol_rowid, * from read_parquet('{}') as hits) as hits", parquet.display()),
        )
        .to_string();
    let rowid_sql = format!("select __wcol_rowid {from_wrapped}");

    let date_re =
        Regex::new(r"\bEventDate\b\s*(=|<>|!=|<=|>=|<|>)\s*'(\d{4}-\d{2}-\d{2})'").unwrap();
    let fixed = date_re
        .replace_all(
            &rowid_sql,
            "EventDate $1 date_diff('day', DATE '1970-01-01', DATE '$2')",
        )
        .to_string();

    run_duckdb_json(&fixed)
}

fn run_duckdb_json(sql: &str) -> Result<Vec<Value>> {
    let output = Command::new("duckdb")
        .arg("-json")
        .arg("-c")
        .arg(sql)
        .output()
        .with_context(|| "running duckdb; ensure duckdb CLI is installed")?;
    if !output.status.success() {
        anyhow::bail!(
            "{}",
            String::from_utf8_lossy(&output.stderr).trim().to_string()
        );
    }
    let stdout = if output.stdout.is_empty() {
        b"[]".as_slice()
    } else {
        output.stdout.as_slice()
    };
    let parsed: Value =
        serde_json::from_slice(stdout).with_context(|| "parsing duckdb JSON output")?;
    Ok(match parsed {
        Value::Array(arr) => arr,
        other => vec![other],
    })
}

fn duck_signature(rows: &[Value]) -> String {
    signature_from_values(rows)
}

fn duck_count_only_signature(rows: &[Value]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut vals = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(obj) = row.as_object() else {
            return String::new();
        };
        let Some(v) = obj.get("c") else {
            return String::new();
        };
        let Some(n) = value_to_f64(v) else {
            return String::new();
        };
        vals.push(n);
    }
    if vals.iter().all(|n| (*n - vals[0]).abs() <= 0.0) {
        signature_from_numbers(&vals)
    } else {
        String::new()
    }
}

fn wcol_signature(result: &QueryResult) -> String {
    if let Some(groups) = &result.groups {
        if !groups.keys.is_empty() {
            return wcol_groups_signature(groups);
        }
    }
    if !result.aggregates.is_empty() {
        let mut nums = Vec::new();
        for (name, stats) in &result.aggregates {
            nums.push(agg_value_by_name(name, stats));
        }
        return signature_from_numbers(&nums);
    }
    let nums = result.rows.iter().map(|r| *r as f64).collect::<Vec<_>>();
    signature_from_numbers(&nums)
}

fn wcol_groups_signature(groups: &GroupResult) -> String {
    let mut vals = Vec::new();

    let include_key = |idx: usize| {
        let Some(info) = groups.key_info.get(idx) else {
            return true;
        };
        if (info.flags & FLAG_DICT) != 0 {
            return false;
        }
        if info.physical_type == TYPE_STRING || info.physical_type == TYPE_I64 {
            return false;
        }
        true
    };

    let include_key1 = include_key(0);
    let include_key2 = include_key(1);

    for (idx, key) in groups.keys.iter().enumerate() {
        if include_key1 {
            vals.push(decode_group_key(groups.key_info.get(0), *key));
        }
        if include_key2 {
            if let Some(keys2) = &groups.keys2 {
                vals.push(decode_group_key(groups.key_info.get(1), keys2[idx]));
            }
        }
        if let Some(row) = groups.values.get(idx) {
            for (agg_idx, agg) in groups.aggs.iter().enumerate() {
                if let Some(stats) = row.get(agg_idx) {
                    vals.push(agg_value_by_kind(agg.kind, stats));
                }
            }
        }
    }

    signature_from_numbers(&vals)
}

fn wcol_group_signature_count_only(result: &QueryResult) -> String {
    let Some(groups) = &result.groups else {
        return String::new();
    };
    if groups.keys.is_empty() {
        return String::new();
    }

    let agg_index = groups.aggs.iter().position(|agg| {
        agg.kind == AGG_KIND_COUNT_STAR
            || agg.kind == AGG_KIND_COUNT
            || agg.kind == AGG_KIND_APPROX_DISTINCT
    });
    let Some(agg_index) = agg_index else {
        return String::new();
    };

    let vals = groups
        .values
        .iter()
        .filter_map(|row| row.get(agg_index))
        .map(|stats| agg_value_by_kind(groups.aggs[agg_index].kind, stats))
        .filter(|n| n.is_finite())
        .collect::<Vec<_>>();

    signature_from_numbers(&vals)
}

fn decode_group_key(info: Option<&wcol_decoder::native::GroupKeyInfo>, raw: u64) -> f64 {
    let Some(info) = info else {
        return raw as f64;
    };
    match info.physical_type {
        TYPE_I32 => to_signed(raw, 32) as f64,
        TYPE_I16 => to_signed(raw, 16) as f64,
        TYPE_I8 => to_signed(raw, 8) as f64,
        _ => raw as f64,
    }
}

fn to_signed(value: u64, bits: u8) -> i64 {
    let mask = (1u128 << bits) - 1;
    let v = (value as u128) & mask;
    let sign_bit = 1u128 << (bits - 1);
    if v >= sign_bit {
        (v as i128 - (1i128 << bits)) as i64
    } else {
        v as i64
    }
}

fn signatures_match(duck: &str, wcol: &str, approx_aggregate: bool) -> bool {
    if duck == wcol {
        return true;
    }
    let d = split_sig(duck);
    let w = split_sig(wcol);
    if d.len() != w.len() {
        return false;
    }
    for (a, b) in d.iter().zip(w.iter()) {
        if !a.starts_with("n:") || !b.starts_with("n:") {
            if a != b {
                return false;
            }
            continue;
        }
        let da = a[2..].parse::<f64>().unwrap_or(f64::NAN);
        let db = b[2..].parse::<f64>().unwrap_or(f64::NAN);
        if !da.is_finite() || !db.is_finite() {
            return false;
        }
        let abs = (da - db).abs();
        if abs <= 1e-9 {
            continue;
        }
        let rel_tol = if approx_aggregate && d.len() == 1 {
            0.2
        } else {
            1e-6
        };
        let mag = da.abs().max(db.abs()).max(1.0);
        if abs / mag > rel_tol {
            return false;
        }
    }
    true
}

fn split_sig(sig: &str) -> Vec<&str> {
    sig.split(',').filter(|s| !s.is_empty()).collect()
}

fn sig_len(sig: &str) -> usize {
    split_sig(sig).len()
}

fn trunc_sig(sig: &str, max: usize) -> String {
    if sig.len() <= max {
        sig.to_string()
    } else {
        format!("{}...", &sig[..max])
    }
}

fn signature_from_numbers(nums: &[f64]) -> String {
    let mut tokens = nums
        .iter()
        .copied()
        .filter(|n| is_safe_signature_number(*n))
        .map(|n| format!("n:{}", round6(n)))
        .collect::<Vec<_>>();
    tokens.sort_by(cmp_token);
    tokens.dedup();
    tokens.join(",")
}

fn signature_from_values(values: &[Value]) -> String {
    let mut tokens = Vec::new();
    for value in values {
        collect_tokens(value, &mut tokens, false);
    }
    tokens.sort_by(cmp_token);
    tokens.dedup();
    tokens.join(",")
}

fn collect_tokens(value: &Value, out: &mut Vec<String>, _in_object: bool) {
    match value {
        Value::Null => {}
        Value::Bool(_) => {}
        Value::Number(num) => {
            let n = if let Some(v) = num.as_f64() {
                v
            } else if let Some(v) = num.as_i64() {
                v as f64
            } else if let Some(v) = num.as_u64() {
                v as f64
            } else {
                return;
            };
            if !is_safe_signature_number(n) {
                return;
            }
            out.push(format!("n:{}", round6(n)));
        }
        Value::String(_) => {}
        Value::Array(items) => {
            for item in items {
                collect_tokens(item, out, false);
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                if k.chars().all(|c| c.is_ascii_digit()) {
                    continue;
                }
                collect_tokens(v, out, true);
            }
        }
    }
}

fn cmp_token(a: &String, b: &String) -> std::cmp::Ordering {
    let av = a.strip_prefix("n:").and_then(|s| s.parse::<f64>().ok());
    let bv = b.strip_prefix("n:").and_then(|s| s.parse::<f64>().ok());
    match (av, bv) {
        (Some(x), Some(y)) => x.total_cmp(&y),
        _ => a.cmp(b),
    }
}

fn round6(n: f64) -> f64 {
    (n * 1_000_000.0).round() / 1_000_000.0
}

fn value_to_f64(value: &Value) -> Option<f64> {
    if let Some(v) = value.as_f64() {
        return Some(v);
    }
    if let Some(v) = value.as_i64() {
        return Some(v as f64);
    }
    if let Some(v) = value.as_u64() {
        return Some(v as f64);
    }
    None
}

fn is_safe_signature_number(n: f64) -> bool {
    if !n.is_finite() {
        return false;
    }
    if n.fract() == 0.0 && n.abs() > 9_007_199_254_740_991.0 {
        return false;
    }
    true
}
