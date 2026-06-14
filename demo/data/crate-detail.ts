import { buildPlan } from "../wcol-query.ts";
import type { CrateDetailSummary, CrateVersionRow } from "../generated/engine-types.ts";

export function crateDetailPlan(crateName: string) {
  return buildPlan({
    filters: [{ column: "crate_name", op: "=", value: crateName }],
    select: ["version", "license", "downloads", "yanked", "edition"],
    limit: 100,
  });
}

type Row = Record<string, string | number | boolean | null>;

export function summarizeCrateDetail(crateName: string, rows: Row[]): CrateDetailSummary {
  const versions: CrateVersionRow[] = rows
    .map((row) => ({
      version: String(row.version ?? "—"),
      license: String(row.license ?? "—"),
      downloads: BigInt(Number(row.downloads ?? 0)),
      yanked: row.yanked === true || row.yanked === "true" || row.yanked === 1,
      edition: row.edition != null && row.edition !== "" ? String(row.edition) : null,
    }))
    .sort((a, b) => Number(b.downloads - a.downloads));

  const totalDownloads = versions.reduce((sum, v) => sum + v.downloads, 0n);
  const yankedCount = versions.filter((v) => v.yanked).length;
  const licenseCounts = new Map<string, number>();
  for (const v of versions) {
    licenseCounts.set(v.license, (licenseCounts.get(v.license) ?? 0) + 1);
  }
  let primaryLicense: string | null = null;
  let best = 0;
  for (const [lic, count] of licenseCounts) {
    if (count > best) {
      best = count;
      primaryLicense = lic;
    }
  }

  return {
    crateName,
    totalDownloads,
    versionCount: versions.length,
    primaryLicense,
    yankedCount,
    versions,
  };
}
