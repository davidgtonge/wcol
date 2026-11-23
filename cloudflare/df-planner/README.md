# df-planner Worker (local spike)

Exposes DataFusion logical planning over HTTP via `df-planner-wasm` (web bindgen + CompiledWasm).

## Local

```bash
npm run df-planner:build-wasm
npx wrangler dev --config cloudflare/df-planner/wrangler.toml --port 8799
```

Bench (cold + warm POST in one isolate):

```bash
WORKER_URL=http://127.0.0.1:8799 npm run df-planner:bench
```

## API

- `GET /` — usage
- `POST /plan/bench` — default ClickBench Q3 + embedded `hits` catalog
- `POST /plan` — body `{ "sql": "...", "catalog": { "table", "columns" } }` (catalog optional)

Response includes `timing`: `wasmModuleLoadMs`, `planMs`, `totalRequestMs` (first request in an isolate includes wasm init).

Deploy: `npx wrangler deploy --config cloudflare/df-planner/wrangler.toml` (requires Cloudflare account).
