import { CombineOp, FilterOp } from "../core/constants.ts";
import type { CombineToken, FilterOpToken } from "../core/types.ts";

export function normalizeOp(op: FilterOpToken | undefined): number {
  switch (canonicalOpToken(op)) {
    case "eq":
      return FilterOp.EQ;
    case "neq":
      return FilterOp.NEQ;
    case "lt":
      return FilterOp.LT;
    case "lte":
      return FilterOp.LTE;
    case "gt":
      return FilterOp.GT;
    case "gte":
      return FilterOp.GTE;
    case "between":
      return FilterOp.BETWEEN;
    case "like":
      return FilterOp.LIKE;
    case "not_like":
      return FilterOp.NOT_LIKE;
    default:
      if (typeof op === "number") {
        return op;
      }
      throw new Error(`Unsupported operator: ${op}`);
  }
}

export function normalizeCombineToken(token: CombineToken): number {
  if (typeof token === "number") {
    return token;
  }
  const value = String(token).toUpperCase();
  if (value === "AND") {
    return CombineOp.AND;
  }
  if (value === "OR") {
    return CombineOp.OR;
  }
  if (value === "NOT") {
    return CombineOp.NOT;
  }
  throw new Error(`Unsupported combine token: ${token}`);
}

export function isInOp(op: FilterOpToken | undefined): boolean {
  return canonicalOpToken(op) === "in";
}

function canonicalOpToken(op: FilterOpToken | undefined): string {
  const raw = String(op ?? "=").toLowerCase().trim();
  switch (raw) {
    case "=":
    case "==":
      return "eq";
    case "!=":
      return "neq";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "not_like":
    case "not like":
      return "not_like";
    default:
      return raw;
  }
}
