export type ColumnRef = number | string;
export type U64 = number | bigint;

export type ColumnInfo = {
  logicalType: number;
  physicalType: number;
  flags: number;
  encoding: number;
  dictId: number;
  scale: number;
};

export type HeaderInfo = {
  version: number;
  flags: number;
  ncols: number;
  nchunks: number;
  rowsPerChunk: number;
  totalRows: U64;
  schemaOff: U64;
  schemaLen: U64;
  indexOff: U64;
  indexLen: U64;
  dictOff: U64;
  dictLen: U64;
  dataOff: U64;
  dictRawLen: U64;
};

export type DictLookup = Map<string, number>;
export type DictsMap = Map<number, DictLookup>;

export type FilterOpToken = number | string;
export type CombineToken = number | string;

export type FilterSpec = {
  column?: ColumnRef;
  col?: ColumnRef;
  op?: FilterOpToken;
  operator?: FilterOpToken;
  value?: unknown;
  value2?: unknown;
};

export type GroupBySpec = {
  keys?: ColumnRef[];
  key?: ColumnRef;
  value?: ColumnRef;
};

export type AggregateSpec = {
  column?: ColumnRef;
  col?: ColumnRef;
};

export type QueryPlan = {
  limit?: number;
  filters?: FilterSpec[];
  combine?: CombineToken[];
  groupBy?: GroupBySpec;
  aggregates?: AggregateSpec[];
  /** When set with groupBy + limit, emit top-K groups by row count (fast path). */
  groupOrderByCount?: boolean;
  /** Row-return projection (incompatible with groupBy / aggregates). */
  select?: ColumnRef[];
};

export const PROJ_KIND_F64 = 0;
export const PROJ_KIND_DICT_ID = 1;
export const PROJ_KIND_BOOL = 2;

export type ProjectionColumnMeta = {
  name: string;
  colId: number;
  kind: number;
};

export type ProjectionColumn =
  | { kind: typeof PROJ_KIND_F64; values: Float64Array; nulls: Uint8Array }
  | { kind: typeof PROJ_KIND_DICT_ID; values: Uint32Array; nulls: Uint8Array }
  | { kind: typeof PROJ_KIND_BOOL; values: Uint8Array; nulls: Uint8Array };

export type RowProjection = {
  columns: ProjectionColumnMeta[];
  data: ProjectionColumn[];
};

export type AggregateStats = {
  count: number;
  sum: number;
  min: number;
  max: number;
  mean: number;
};

export type GroupResult = {
  keys: U64[];
  keys2?: U64[];
  keyInfo?: GroupKeyInfo[];
  aggs: GroupAggInfo[];
  values: AggregateStats[][];
};

export type QueryResult = {
  rows: U64[];
  projection: RowProjection | null;
  aggregates: Record<string, AggregateStats>;
  groups: GroupResult | null;
};

export type QueryOptions = {
  workers?: number;
};

export type ExecuteOptions = {
  workers?: number;
  sql?: string;
};

export type WorkerRuntimeKind = "node" | "browser" | "local";

export type RuntimeInitBytes = {
  header: Uint8Array;
  schema: Uint8Array;
  toc: Uint8Array;
  dicts?: Uint8Array;
};

export type GroupKeyInfo = {
  colId: number;
  physicalType: number;
  flags: number;
};

export type GroupAggInfo = {
  colId: number;
  kind: number;
};

export type PageRequest = {
  kind: number;
  colId: number;
  offset: number;
  compLen: number;
  rawLen: number;
};

export type PageRequestList = PageRequest[] & { skip?: boolean };

export type ColumnNameResolver = {
  getColumnName(colId: number): Promise<string>;
};
