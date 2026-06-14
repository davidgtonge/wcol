use crate::state::{QueryDraft, QueryMode};
use serde_json::{json, Value};

const DEFAULT_TOP_K: u32 = 25;

pub fn plan_preview(draft: &QueryDraft) -> String {
    match build_query_plan(draft) {
        Ok(plan) => serde_json::to_string_pretty(&plan).unwrap_or_else(|_| "{}".into()),
        Err(message) => serde_json::to_string_pretty(&json!({ "error": message }))
            .unwrap_or_else(|_| format!("{{\"error\":\"{message}\"}}")),
    }
}

pub fn build_query_plan(draft: &QueryDraft) -> Result<Value, String> {
    let mut filters: Vec<Value> = draft
        .filters
        .iter()
        .filter(|f| !f.column.is_empty() && !f.value.trim().is_empty())
        .map(|f| filter_to_json(f))
        .collect::<Result<_, _>>()?;

    let search = draft.search_text.trim();
    if !search.is_empty() && !draft.search_column.is_empty() {
        filters.insert(
            0,
            json!({
                "column": draft.search_column,
                "op": "like",
                "value": search
            }),
        );
    }

    let combine = combine_tokens(filters.len());
    let top_k = if draft.top_k > 0 {
        draft.top_k
    } else {
        DEFAULT_TOP_K
    };

    if draft.mode == QueryMode::Aggregate {
        let keys: Vec<&str> = draft
            .group_keys
            .iter()
            .filter(|k| !k.is_empty())
            .take(2)
            .map(String::as_str)
            .collect();
        if keys.is_empty() {
            return Err("Pick at least one group-by column".into());
        }
        let agg_col = if draft.agg_column.is_empty() {
            "downloads"
        } else {
            draft.agg_column.as_str()
        };
        let mut plan = json!({
            "limit": top_k,
            "groupBy": { "keys": keys, "value": agg_col },
            "aggregates": [{ "column": agg_col }],
            "groupOrderByCount": true
        });
        if !filters.is_empty() {
            plan["filters"] = json!(filters);
            if let Some(c) = combine {
                plan["combine"] = json!(c);
            }
        }
        return Ok(plan);
    }

    let select = if draft.mode == QueryMode::Table && !draft.select_columns.is_empty() {
        Some(draft.select_columns.clone())
    } else {
        None
    };

    let mut plan = json!({ "limit": top_k });
    if !filters.is_empty() {
        plan["filters"] = json!(filters);
        if let Some(c) = combine {
            plan["combine"] = json!(c);
        }
    }
    if let Some(cols) = select {
        plan["select"] = json!(cols);
    }
    Ok(plan)
}

fn combine_tokens(filter_count: usize) -> Option<Vec<&'static str>> {
    if filter_count <= 1 {
        None
    } else {
        Some(vec!["AND"; filter_count - 1])
    }
}

fn filter_to_json(f: &crate::state::FilterDraft) -> Result<Value, String> {
    let op = if f.op == "contains" { "like" } else { f.op.as_str() };
    if f.op == "between" {
        let parts: Vec<&str> = f.value.split(',').map(str::trim).collect();
        let a = parts.first().copied().unwrap_or("");
        let b = parts.get(1).copied().unwrap_or("");
        return Ok(json!({
            "column": f.column,
            "op": "between",
            "value": a,
            "value2": b
        }));
    }
    Ok(json!({
        "column": f.column,
        "op": op,
        "value": parse_filter_value(&f.value, &f.op)
    }))
}

fn parse_filter_value(raw: &str, op: &str) -> Value {
    let t = raw.trim();
    if op == "in" {
        return json!(t
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>());
    }
    if let Ok(n) = t.parse::<f64>() {
        if t.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
            return json!(n);
        }
    }
    if t == "true" {
        return json!(true);
    }
    if t == "false" {
        return json!(false);
    }
    json!(t)
}
