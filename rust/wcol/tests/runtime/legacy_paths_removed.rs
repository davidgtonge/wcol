//! I-14: removed Rust duplicate names stay out of crate `src/`.

use std::fs;
use std::path::PathBuf;

const FORBIDDEN_IN_SRC: &[&str] = &[
    "pub type ColocatedQueryGraph",
    "pub type QueryPipeline",
    "as ExecutionGroup",
    "encode_ingest_query_chain",
    "FilterWcsOp",
    "AggregateWcsOp",
    "ColocatedGraphWire",
    "colocated_graph_wire",
    "encode_ingest_query_plan",
    "plan_row_export",
    "OP_COLOCATED_PARQUET_QUERY",
    "struct ColocatedGraphPlan",
];

#[test]
fn rust_src_has_no_retired_legacy_symbols() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut hits = Vec::new();
    for src in [
        root.join("wcol/src"),
        root.join("wcol-engine/src"),
        root.join("wcol-plan/src"),
    ] {
        walk_rs(&src, &mut |path, text| {
            let rel = path.strip_prefix(&root).unwrap().display();
            for needle in FORBIDDEN_IN_SRC {
                if text.contains(needle) {
                    hits.push(format!("{rel}: contains `{needle}`"));
                }
            }
        });
    }
    assert!(
        hits.is_empty(),
        "I-14 legacy symbols in src:\n{}",
        hits.join("\n")
    );
}

/// I2: future engine modules do not parse SQL or open operators from SQL text.
#[test]
fn engine_modules_have_no_sql_entry() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../wcol-engine/src");
    let engine = ["formats", "stream", "ops", "exec", "ir", "program"];
    let mut hits = Vec::new();
    for dir_name in engine {
        let dir = src.join(dir_name);
        if !dir.exists() {
            hits.push(format!("missing engine dir {dir_name}"));
            continue;
        }
        walk_rs(&dir, &mut |path, text| {
            let rel = path.strip_prefix(&src).unwrap().display().to_string();
            for (line_no, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//")
                    || trimmed.starts_with("///")
                    || trimmed.starts_with("//!")
                {
                    continue;
                }
                if line.contains("plan::sql_parser")
                    || line.contains("plan::sql::")
                    || line.contains("plan::sql;")
                    || line.contains("fn open(sql:")
                {
                    hits.push(format!("{rel}:{}: {trimmed}", line_no + 1));
                }
            }
        });
    }
    assert!(
        hits.is_empty(),
        "I2 engine SQL entry points:\n{}",
        hits.join("\n")
    );
}

fn walk_rs(dir: &PathBuf, f: &mut dyn FnMut(&PathBuf, &str)) {
    for entry in fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, f);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let text = fs::read_to_string(&path).expect("read");
            f(&path, &text);
        }
    }
}
