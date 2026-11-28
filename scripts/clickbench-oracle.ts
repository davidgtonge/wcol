/**
 * How the ladder checks a ClickBench answer against DuckDB. A query's oracle is DuckDB SQL
 * that has one right answer to compare with ours:
 *
 * - `approx_count_distinct` is computed exactly, and our estimate may be off by `tolerance`.
 *   The two engines' sketches differ, so comparing estimate with estimate checks nothing.
 * - A query with no `ORDER BY` has no row order, so rows compare as a multiset.
 * - A top-k with ties at the cut has many right answers. WCOL keeps the smallest output
 *   values among tied rows, so the oracle breaks ties by every output column in order.
 * - `EventDate` is stored as days since 1970-01-01 (UINT16), and WCOL compares it with date
 *   literals as day numbers; DuckDB will not cast the literal, so it gets the day number.
 */

export type Oracle = {
  readonly sql: string;
  readonly ordered: boolean;
  /** Output columns that are our estimates, allowed `APPROX_TOLERANCE` relative error. */
  readonly estimates: readonly number[];
};

const APPROX_TOLERANCE = 0.05;
const APPROX_SLACK = 2;

export function oracleFor(sql: string): Oracle {
  const estimates = selectList(sql).flatMap((item, index) => (/\bapprox_count_distinct\s*\(/i.test(item) ? [index] : []));
  const ordered = /\border\s+by\b/i.test(sql);
  let oracle = sql.replace(/\bapprox_count_distinct\s*\(\s*([^)]+?)\s*\)/gi, "count(DISTINCT $1)");
  oracle = oracle.replace(
    /(EventDate\s*(?:>=|<=|<>|=|<|>)\s*)'(\d{4}-\d{2}-\d{2})'/g,
    (_, head: string, date: string) => `${head}${Date.parse(`${date}T00:00:00Z`) / 86_400_000}`,
  );
  if (ordered && /\blimit\b/i.test(oracle)) oracle = withTieBreak(oracle);
  // An order by estimates is not one DuckDB's exact counts must share, and rows tied on a
  // key that is not an output column may come in any order, so neither is checked in place.
  const checkOrder = ordered && estimates.length === 0 && orderKeysSelected(sql);
  return { sql: oracle, ordered: checkOrder, estimates };
}

/** Whether every top-level ORDER BY key is an output column: an ordinal, alias or item. */
function orderKeysSelected(sql: string): boolean {
  const order = /\border\s+by\s+([\s\S]*?)(?:\s+(?:limit|offset)\b|;|$)/i.exec(sql);
  if (!order) return true;
  const normal = (text: string) => text.trim().replace(/\s+/g, " ").toLowerCase();
  const outputs = selectList(sql).flatMap((item) => {
    const alias = /\s+as\s+(\w+)\s*$/i.exec(item);
    return alias ? [normal(alias[1]!), normal(item.slice(0, alias.index))] : [normal(item)];
  });
  return order[1]!
    .split(",")
    .map((key) => normal(key.replace(/\s+(asc|desc)\s*$/i, "")))
    .every((key) => /^\d+$/.test(key) || outputs.includes(key));
}

/** Append `, 1, 2, …, n` (every output column) to the top-level ORDER BY. */
function withTieBreak(sql: string): string {
  const columns = selectList(sql).length;
  const positions = Array.from({ length: columns }, (_, index) => index + 1).join(", ");
  return sql.replace(/(\border\s+by\s+[\s\S]*?)(\s+(?:limit|offset)\b)/i, `$1, ${positions}$2`);
}

/** Top-level comma-separated items between SELECT and FROM. */
function selectList(sql: string): string[] {
  const body = sql.slice(sql.search(/\bselect\b/i) + "select".length);
  const items: string[] = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < body.length; index++) {
    const char = body[index];
    if (char === "(") depth += 1;
    else if (char === ")") depth -= 1;
    else if (depth === 0 && char === ",") {
      items.push(body.slice(start, index));
      start = index + 1;
    } else if (depth === 0 && /^\sfrom\s/i.test(body.slice(index, index + 6))) {
      items.push(body.slice(start, index));
      return items;
    }
  }
  return items;
}

function cellMatches(ours: string, expected: string, estimate: boolean): boolean {
  if (ours === expected) return true;
  const a = Number(ours);
  const b = Number(expected);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
  // A sketch of a few dozen values can be off by one or two, which is more than 5%.
  return estimate ? Math.abs(a - b) <= Math.max(APPROX_TOLERANCE * Math.abs(b), APPROX_SLACK) : a === b;
}

const byText = (left: string[], right: string[]): number => {
  const a = left.join("\u0000");
  const b = right.join("\u0000");
  return a < b ? -1 : a > b ? 1 : 0;
};

/** Ordered rows compare in place; unordered ones sorted by their exact cells first. */
export function rowsMatch(ours: string[][], expected: string[][], oracle: Oracle): boolean {
  if (ours.length !== expected.length) return false;
  const estimate = (col: number) => oracle.estimates.includes(col);
  const exact = (row: string[]) => row.filter((_, col) => !estimate(col));
  const sorted = (rows: string[][]) => [...rows].sort((left, right) => byText(exact(left), exact(right)));
  const [a, b] = oracle.ordered ? [ours, expected] : [sorted(ours), sorted(expected)];
  return a.every(
    (row, index) => row.length === b[index]!.length && row.every((cell, col) => cellMatches(cell, b[index]![col]!, estimate(col))),
  );
}
