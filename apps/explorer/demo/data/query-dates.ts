/** ISO date (YYYY-MM-DD) → days since Unix epoch (DuckDB DATE / wcol numeric dates). */
export function epochDay(iso: string): number {
  const ms = Date.parse(`${iso}T00:00:00Z`);
  if (Number.isNaN(ms)) {
    throw new Error(`Invalid ISO date: ${iso}`);
  }
  return Math.floor(ms / 86_400_000);
}

/** Last N days of the trends dump (ends 2026-06-03). */
export const TRENDS_LAST_30D_CUTOFF = epochDay("2026-05-04");
export const TRENDS_LAST_WEEK_CUTOFF = epochDay("2026-05-27");
export const TRENDS_JUNE_CUTOFF = epochDay("2026-06-01");
export const TRENDS_MAY_CUTOFF = epochDay("2026-05-01");
export const TRENDS_MID_MAY_CUTOFF = epochDay("2026-05-15");
