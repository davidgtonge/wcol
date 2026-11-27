/**
 * List objects in an R2 bucket via the Cloudflare API, with the Wrangler OAuth token
 * (macOS location) and the account in `CF_ACCOUNT_ID`.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

function wranglerConfigPath(): string {
  return path.join(os.homedir(), "Library", "Preferences", ".wrangler", "config", "default.toml");
}

export function readWranglerOAuthToken(): string {
  const text = fs.readFileSync(wranglerConfigPath(), "utf8");
  const m = text.match(/oauth_token\s*=\s*"([^"]+)"/);
  if (!m) throw new Error(`no oauth_token in ${wranglerConfigPath()} — run wrangler login`);
  return m[1]!;
}

export type R2ListedObject = {
  key: string;
  size: number;
  lastModified?: string;
};

export async function listR2Objects(
  bucket: string,
  prefix: string,
  options?: { accountId?: string; limit?: number },
): Promise<R2ListedObject[]> {
  const accountId = options?.accountId ?? process.env.CF_ACCOUNT_ID;
  if (!accountId) throw new Error("set CF_ACCOUNT_ID to the Cloudflare account that owns the bucket");
  const token = readWranglerOAuthToken();
  const limit = options?.limit ?? 1000;
  const out: R2ListedObject[] = [];
  let cursor: string | undefined;
  for (;;) {
    const params = new URLSearchParams({ prefix, limit: String(limit) });
    if (cursor) params.set("cursor", cursor);
    const url = `https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${bucket}/objects?${params}`;
    const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`R2 list failed (${response.status}): ${body.slice(0, 300)}`);
    }
    const json = (await response.json()) as {
      result?: Array<{ key: string; size: number; last_modified?: string }>;
      result_info?: { is_truncated?: boolean; cursor?: string };
    };
    for (const obj of json.result ?? []) {
      out.push({
        key: obj.key,
        size: obj.size,
        lastModified: obj.last_modified,
      });
    }
    if (!json.result_info?.is_truncated) break;
    cursor = json.result_info.cursor;
    if (!cursor) break;
  }
  return out;
}
