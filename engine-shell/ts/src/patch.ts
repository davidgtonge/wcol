import type { Path, ViewModelPatch } from "./types";

export function applyPatches<T>(vm: T, patches: ViewModelPatch[]): T {
  if (patches.length === 0) return vm;
  let next: unknown = vm;
  for (const patch of patches) {
    next = applyOne(next, patch);
  }
  return next as T;
}

function applyOne(root: unknown, patch: ViewModelPatch): unknown {
  if (patch.path.length === 0) {
    if (patch.op === "replace") return patch.value;
    throw new Error("cannot remove root view model");
  }
  return mutateAt(root, patch.path, 0, patch);
}

function mutateAt(node: unknown, path: Path, depth: number, patch: ViewModelPatch): unknown {
  const head = path[depth]!;
  const isLast = depth === path.length - 1;

  if (Array.isArray(node)) {
    const arr = node.slice();
    if (isLast) {
      applyLeaf(arr, head, patch);
      return arr;
    }
    const idx = head as number;
    arr[idx] = mutateAt(arr[idx], path, depth + 1, patch);
    return arr;
  }

  if (isPlainObject(node)) {
    const obj = { ...node } as Record<string, unknown>;
    const key = String(head);
    if (isLast) {
      applyLeaf(obj, key, patch);
      return obj;
    }
    obj[key] = mutateAt(obj[key], path, depth + 1, patch);
    return obj;
  }

  throw new Error(`cannot patch at ${String(head)}`);
}

function applyLeaf(
  container: Record<string, unknown> | unknown[],
  key: string | number,
  patch: ViewModelPatch,
): void {
  if (patch.op === "remove") {
    if (Array.isArray(container)) {
      container.splice(key as number, 1);
      return;
    }
    delete container[key as string];
    return;
  }

  const value = patch.value;
  if (Array.isArray(container)) {
    const idx = key as number;
    if (patch.op === "insert") {
      container.splice(idx, 0, value);
      return;
    }
    if (idx === container.length) {
      container.push(value);
      return;
    }
    container[idx] = value;
    return;
  }

  container[key as string] = value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
