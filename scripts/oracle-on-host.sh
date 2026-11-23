#!/bin/bash
# Compute the ladder's full-hits DuckDB answers on a remote host with the DuckDB CLI, and
# store them in data/oracle-cache/ for `ORACLE=cached` runs. Full hits does not fit a laptop.
#
#   scripts/oracle-on-host.sh <ssh-host> [remote-dir]
#
# The remote dir must hold `duckdb` (the CLI) and `hits.parquet`
# (https://datasets.clickhouse.com/hits_compatible/hits.parquet). Answers already on the
# host are reused; delete <remote-dir>/out to recompute.
set -euo pipefail
host=$1
dir=${2:-/root/clickbench}
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

WCOL_BASE_URL=unused STAGE=c CLICKBENCH_TSV=1 ORACLE=print npx tsx "$root/scripts/cf-production-jobs.ts" \
  | sed "s#read_parquet('https://datasets.clickhouse.com/hits_compatible/hits.parquet')#read_parquet('$dir/hits.parquet')#" \
  > "$work/queries.tsv"

scp -q "$work/queries.tsv" "$host:$dir/queries.tsv"
ssh "$host" "cd $dir && mkdir -p out spill && while IFS=\$'\t' read -r key name sql; do
  [ -s out/\$key.csv ] && continue
  if ./duckdb -csv -c \"SET memory_limit='32GB'; SET temp_directory='$dir/spill'; \$sql\" > out/\$key.tmp 2> out/\$key.err; then
    mv out/\$key.tmp out/\$key.csv; echo \"\$name ok\"
  else
    echo \"\$name FAILED: \$(head -c 200 out/\$key.err)\"
  fi
done < queries.tsv"
scp -q -r "$host:$dir/out" "$work/out"

python3 - "$work" "$root/data/oracle-cache" <<'PY'
import csv, io, json, os, sys
work, cache = sys.argv[1], sys.argv[2]
for line in open(os.path.join(work, "queries.tsv")):
    key, name = line.split("\t")[:2]
    src = os.path.join(work, "out", key + ".csv")
    if not os.path.exists(src):
        print(name, "no answer"); continue
    # The header row is dropped; DuckDB quotes some values, so parse it as CSV.
    rows = list(csv.reader(io.StringIO(open(src).read())))[1:]
    json.dump(rows, open(os.path.join(cache, key + ".json"), "w"))
PY
echo "answers stored in $root/data/oracle-cache"
