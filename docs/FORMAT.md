# wcol file format (v7)

The browser engine and `wcol-cli` only read **version 7** files. Re-encode older `.wcol` with current `wcol-cli`.

## Header (96 bytes, little-endian)

| Offset | Field |
|--------|--------|
| 0 | `version` = 7 |
| 4 | `flags` |
| 8 | `ncols` |
| 12 | `nchunks` |
| 16 | `rows_per_chunk` = **65504** |
| 20 | `total_rows` (u64) |
| 28 | `schema_off` (u64) |
| 36 | `schema_len` (u64) |
| 44 | `index_off` (u64) |
| 52 | `index_len` (u64) |
| 60 | `dict_off` (u64) |
| 68 | `dict_len` (u64) |
| 76 | `data_off` (u64) |
| 84 | `dict_raw_len` (u64) |

Constants live in `rust/wcol-format` (`WCOL_VERSION`, `HEADER_BYTES`, `ROWS_PER_CHUNK`, `INDEX_ENTRY_BYTES`).

## Schema

Column names are **u16** length-prefixed UTF-8 (no v6 u8 names).

## Chunk index

Each chunk has `ncols` index entries of **80 bytes** (`INDEX_ENTRY_BYTES`). Layout matches the v7 entry in the encoder.

## String columns (Option A)

Raw string pages use **Option A** layout only:

- `suffix_len_width` byte has bit `0x80` set.
- Row ids map to unique suffix entries (not legacy sorted-permutation + bloom blocks).

Legacy bloom-skipped LIKE paths and pre–Option A layouts are removed from the decoder; see `archive/experimental/` if needed.

## Errors (WASM / FFI)

| Code | Meaning |
|------|---------|
| `-2` | Bad header (not v7 or wrong size) |
| `-128` | String page missing Option A flag |
| `-126` | Legacy string header bits (bloom / mask flags) |
