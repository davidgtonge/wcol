export const HEADER_BYTES = 96;
export const HEADER_INFO_BYTES = 92;
export const COLUMN_INFO_BYTES = 12;
export const CHUNK_SPAN_BYTES = 12;
export const PAGE_REQ_WORDS = 6;
export const PAGE_EXEC_WORDS = 5;
/** wcol v7 chunk index entry size */
export const INDEX_ENTRY_BYTES = 80;
export const FLAG_DICT = 2;
export const HEADER_FLAG_DICT_COMPRESSED = 1;
export const TYPE_STRING = 10;

export const textDecoder = new TextDecoder("utf-8");
export const EMPTY_U8 = new Uint8Array();

export const FilterOp = Object.freeze({
  EQ: 0,
  NEQ: 1,
  LT: 2,
  LTE: 3,
  GT: 4,
  GTE: 5,
  BETWEEN: 6,
  LIKE: 7,
  NOT_LIKE: 8
});

export const CombineOp = Object.freeze({
  AND: -1,
  OR: -2,
  NOT: -3
});
