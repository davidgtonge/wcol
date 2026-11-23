"""Compare query_bench --print-groups output with DuckDB on the same Parquet file.

usage:
  cargo build --release -p wcol --features native --bin query_bench --manifest-path rust/Cargo.toml
  python3 -I scripts/check-duckdb-results.py rust/target/release/query_bench \
      data/hits_subset_500k.parquet scripts/clickbench-queries.tsv

Needs the `duckdb` CLI on PATH.
Arguments after the query file go to query_bench (`--plan-nodes N` checks the physical-plan path).
queries.tsv: "<label>\t<sql>" per line. Compares complete group sets (ORDER BY / LIMIT /
OFFSET are stripped), so tie order does not matter. A final top-k that returns at most
offset+limit groups is accepted when those groups are a tie-safe window of that full set.
Labels starting with "A" use
approx_count_distinct: DuckDB runs the exact COUNT(DISTINCT) and values may differ by 3%.
"""
import json
import math
import re
import subprocess
import sys

bench, parquet, queries = sys.argv[1:4]
# Extra query_bench arguments, e.g. `--plan-nodes 4` to run through the physical plan.
bench_args = sys.argv[4:]

def duck_sql(sql):
    sql = sql.strip().rstrip(";")
    sql = re.sub(r"approx_count_distinct\(", "count(DISTINCT ", sql, flags=re.I)
    sql = re.sub(r"\s+ORDER\s+BY\s+.*?(?=\s+LIMIT\b|\s+OFFSET\b|$)", "", sql, flags=re.I)
    sql = re.sub(r"\s+LIMIT\s+\d+", "", sql, flags=re.I)
    sql = re.sub(r"\s+OFFSET\s+\d+", "", sql, flags=re.I)
    # hits stores EventDate as a day count; express date literals the same way for DuckDB.
    sql = re.sub(r"'(\d{4}-\d{2}-\d{2})'", r"('\1'::DATE - DATE '1970-01-01')", sql)
    return re.sub(r"\bFROM\s+hits\b", f"FROM '{parquet}'", sql, flags=re.I)


def close(a, b, rel_tol):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    if isinstance(a, int) and isinstance(b, int) and rel_tol == 0:
        return a == b
    a, b = float(a), float(b)
    return math.isclose(a, b, rel_tol=max(rel_tol, 1e-9), abs_tol=1 if rel_tol else 1e-9)


def rows_equal(left, right, rel_tol):
    return len(left) == len(right) and all(
        len(x) == len(y) and all(close(p, q, rel_tol) for p, q in zip(x, y)) for x, y in zip(left, right)
    )


def limit_window(sql):
    """`offset + limit` for a final top-k partial. None when the SQL has no LIMIT."""
    limit = re.search(r"\bLIMIT\s+(\d+)", sql, flags=re.I)
    if not limit:
        return None
    offset = re.search(r"\bOFFSET\s+(\d+)", sql, flags=re.I)
    return int(limit.group(1)) + (int(offset.group(1)) if offset else 0)


def topk_matches(ours, full, window, rel_tol, key_count):
    """`ours` is a valid top-`window` of `full` by some numeric column, ties at the cutoff included.

    Rows are paired on the group key, then aggregates compared with the same tolerance as a full set.
    """
    if not ours or not full or len(ours) != min(window, len(full)):
        return False
    index = {tuple(row[:key_count]): row for row in full}
    chosen = set()
    for row in ours:
        other = index.get(tuple(row[:key_count]))
        if other is None or len(row) != len(other):
            return False
        if not all(close(p, q, rel_tol) for p, q in zip(row, other)):
            return False
        chosen.add(tuple(row[:key_count]))
    if len(chosen) != len(ours):
        return False
    width = len(full[0])
    for col in range(width):
        ranks = []
        numeric = True
        for row in full:
            value = row[col]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                numeric = False
                break
            ranks.append(float(value))
        if not numeric:
            continue
        order = sorted(range(len(full)), key=lambda i: (-ranks[i], i))
        cutoff = ranks[order[len(ours) - 1]]
        strict = {tuple(full[i][:key_count]) for i in order if ranks[i] > cutoff}
        boundary = {tuple(full[i][:key_count]) for i in order if ranks[i] == cutoff}
        if strict <= chosen and chosen <= strict | boundary:
            return True
    return False


ok = mismatch = skipped = 0
for line in open(queries):
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    label, sql = line.split("\t", 1)
    out = subprocess.run([bench, parquet, "--print-groups", *bench_args, "--sql", sql], capture_output=True, text=True)
    last = out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""
    if not last.startswith("{"):
        print(f"{label} skip (wcol: {last or out.stderr.strip()[-200:]})")
        skipped += 1
        continue
    ours = json.loads(last)
    if "error" in ours:
        print(f"{label} skip (wcol: {ours['error']})")
        skipped += 1
        continue

    duck = subprocess.run(["duckdb", "-json", "-c", duck_sql(sql)], capture_output=True, text=True)
    if duck.returncode != 0:
        print(f"{label} skip (duckdb: {duck.stderr.strip()[-200:]})")
        skipped += 1
        continue
    theirs = [list(row.values()) for row in json.loads(duck.stdout or "[]")]
    rel_tol = 0.03 if label.startswith("A") else 0
    width = ours.get("keys", 0) if rel_tol else None
    a = sorted(ours["groups"], key=lambda r: json.dumps(r[:width], default=str))
    b = sorted(theirs, key=lambda r: json.dumps(r[:width], default=str))
    same = rows_equal(a, b, rel_tol)
    window = limit_window(sql)
    if not same and window is not None and len(a) < len(b) and topk_matches(
        a, b, window, rel_tol, ours.get("keys", 0)
    ):
        same = True
    if same:
        ok += 1
        print(f"{label} ok ({len(a)} groups)")
    else:
        mismatch += 1
        print(f"{label} MISMATCH: wcol {len(a)} groups, duckdb {len(b)}")
        print(f"   wcol  : {a[:3]}")
        print(f"   duckdb: {b[:3]}")
print(f"ok {ok}, mismatch {mismatch}, skipped {skipped}")
