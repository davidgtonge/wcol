import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { dirname, resolve, join } from "node:path";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(scriptDir, "..");
const wasmDir = resolve(rootDir, "rust", "wcol-wasm");
const wasmBindgenLiteBuildJs = resolve(
  rootDir,
  "node_modules/wasm-bindgen-lite/src/cli/build.js"
);

const wasmFeatures = process.env.WCOL_WASM_FEATURES;
const wasmFeaturesSimd = process.env.WCOL_WASM_FEATURES_SIMD;
const wasmFeaturesBase = process.env.WCOL_WASM_FEATURES_BASE;
const wasmBenchApi = process.env.WCOL_WASM_BENCH_API === "1";
const wasmSqlApi = process.env.WCOL_WASM_SQL_API === "1";

function run(command, args, cwd, env = process.env) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit", env });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function applyReleaseProfileEnv() {
  const profile = process.env.WCOL_WASM_PROFILE?.trim().toLowerCase();
  process.env.CARGO_PROFILE_RELEASE_OPT_LEVEL =
    process.env.WCOL_WASM_OPT_LEVEL ??
    (profile === "size" ? "z" : "3");
  process.env.CARGO_PROFILE_RELEASE_STRIP =
    process.env.WCOL_WASM_STRIP ?? "symbols";
  process.env.CARGO_PROFILE_RELEASE_LTO =
    process.env.WCOL_WASM_LTO ?? "fat";
  process.env.CARGO_PROFILE_RELEASE_CODEGEN_UNITS = "1";
  process.env.CARGO_PROFILE_RELEASE_PANIC = "abort";

  const extraRustflags = [
    process.env.RUSTFLAGS,
    process.env.WCOL_WASM_RUSTFLAGS,
    "-C link-arg=--gc-sections",
  ]
    .filter(Boolean)
    .join(" ")
    .trim();
  if (extraRustflags) {
    process.env.RUSTFLAGS = extraRustflags;
  }
}

// wasm-bindgen-lite hardcodes opt-level=3; teach it to honor our release env.
function patchWasmBindgenLiteReleaseProfile() {
  let src;
  try {
    src = readFileSync(wasmBindgenLiteBuildJs, "utf8");
  } catch {
    console.warn("wasm-bindgen-lite build.js not found; skipping size patch");
    return;
  }

  let changed = false;

  const patchedOpt =
    "env.CARGO_PROFILE_RELEASE_OPT_LEVEL = process.env.CARGO_PROFILE_RELEASE_OPT_LEVEL || '3'";
  const legacyPatchedOpt =
    "env.CARGO_PROFILE_RELEASE_OPT_LEVEL = process.env.CARGO_PROFILE_RELEASE_OPT_LEVEL || 'z'";

  if (src.includes(legacyPatchedOpt)) {
    src = src.replace(legacyPatchedOpt, patchedOpt);
    changed = true;
  } else if (!src.includes(patchedOpt)) {
    if (!src.includes("env.CARGO_PROFILE_RELEASE_OPT_LEVEL = '3'")) {
      console.warn(
        "wasm-bindgen-lite build.js changed; release opt-level patch skipped"
      );
    } else {
      src = src.replace(
        "env.CARGO_PROFILE_RELEASE_OPT_LEVEL = '3'",
        patchedOpt
      );
      changed = true;
    }
  }

  if (
    !src.includes("CARGO_PROFILE_RELEASE_STRIP") &&
    src.includes("env.CARGO_PROFILE_RELEASE_LTO = 'fat'")
  ) {
    src = src.replace(
      "env.CARGO_PROFILE_RELEASE_LTO = 'fat'",
      `env.CARGO_PROFILE_RELEASE_LTO = 'fat'\n    env.CARGO_PROFILE_RELEASE_STRIP = process.env.CARGO_PROFILE_RELEASE_STRIP || 'symbols'`
    );
    changed = true;
  }

  const stripMarker = "wcol: keep target features for simd/bulk-memory/nontrapping fptoint";
  if (
    src.includes("args.push('--strip-target-features')") &&
    !src.includes(stripMarker)
  ) {
    src = src.replace(
      "args.push('--strip-target-features')",
      `// ${stripMarker}`
    );
    changed = true;
  }

  if (changed) {
    writeFileSync(wasmBindgenLiteBuildJs, src);
  }
}

function withFeature(existing, feature) {
  const items = String(existing ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (!items.includes(feature)) {
    items.push(feature);
  }
  return items.join(",");
}

/** WCOL_WASM_PROFILE: speed → wasm -O3; size → Rust z + wasm -Oz; default → Rust 3 + wasm -Oz. */
function resolveWasmOptPass() {
  const profile = process.env.WCOL_WASM_PROFILE?.trim().toLowerCase();
  if (profile === "speed") {
    return "-O3";
  }
  if (profile === "size") {
    return "-Oz";
  }

  const raw = process.env.WCOL_WASM_WASMOPT?.trim();
  if (!raw) {
    return null;
  }
  const normalized = raw.toLowerCase();
  if (normalized === "3" || normalized === "o3") {
    return "-O3";
  }
  if (normalized === "z" || normalized === "oz") {
    return "-Oz";
  }
  if (normalized === "s" || normalized === "os") {
    return "-Os";
  }
  if (raw.startsWith("-O")) {
    return raw;
  }
  console.warn(`Unknown WCOL_WASM_WASMOPT=${raw}; keeping config default`);
  return null;
}

function loadConfigOverrides() {
  const configPath = resolve(wasmDir, "wasm-bindgen-lite.config.json");
  const config = JSON.parse(readFileSync(configPath, "utf8"));
  let changed = false;

  config.targets = config.targets ?? {};
  let simdFeatures = wasmFeaturesSimd ?? wasmFeatures ?? null;
  let baselineFeatures = wasmFeaturesBase ?? wasmFeatures ?? null;
  if (wasmBenchApi) {
    simdFeatures = withFeature(simdFeatures, "bench_api");
    baselineFeatures = withFeature(baselineFeatures, "bench_api");
    changed = true;
  }
  if (wasmSqlApi) {
    simdFeatures = withFeature(simdFeatures, "sql_api");
    baselineFeatures = withFeature(baselineFeatures, "sql_api");
    changed = true;
  }
  if (wasmFeaturesSimd?.trim() || wasmFeaturesBase?.trim() || wasmFeatures?.trim()) {
    changed = true;
  }
  config.targets.simdFeatures = simdFeatures;
  config.targets.baselineFeatures = baselineFeatures;

  const wasmOptPass = resolveWasmOptPass();
  if (wasmOptPass) {
    config.wasmOpt = config.wasmOpt ?? { mode: "auto", args: [] };
    const args = [...(config.wasmOpt.args ?? [])];
    if (args.length > 0 && args[0].startsWith("-O")) {
      args[0] = wasmOptPass;
    } else {
      args.unshift(wasmOptPass);
    }
    config.wasmOpt.args = args;
    changed = true;
  }

  if (!changed) {
    return null;
  }
  const tempPath = join(
    tmpdir(),
    `wcol-wasm-config-${process.pid}-${Date.now()}.json`
  );
  writeFileSync(tempPath, JSON.stringify(config, null, 2));
  return tempPath;
}

applyReleaseProfileEnv();
patchWasmBindgenLiteReleaseProfile();

const args = ["wasm-bindgen-lite", "build", "--crate", ".", "--release"];
const configPath = loadConfigOverrides();
if (configPath) {
  args.push("--config", configPath);
}

run("npx", args, wasmDir);
