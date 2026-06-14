/** Query plan shape (mirrors wcol runtime; safe to import without loading Wasm). */
export type QueryPlan = {
  limit?: number;
  filters?: {
    column?: string;
    col?: string;
    op?: string;
    operator?: string;
    value?: unknown;
    value2?: unknown;
  }[];
  combine?: (string | number)[];
  groupBy?: {
    key?: string;
    keys?: string[];
    value?: string;
  };
  aggregates?: { column?: string; col?: string }[];
  select?: string[];
  groupOrderByCount?: boolean;
};
