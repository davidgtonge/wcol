/** Attempt-scoped task identity for graph execution and ledger fencing. */

export type TaskIdentity = {
  queryId: string;
  taskId: number;
  attempt: number;
};

export function taskIdentityKey(identity: TaskIdentity): string {
  return `${identity.queryId}:${identity.taskId}:${identity.attempt}`;
}

export type TaskCompletionRecord = {
  identity: TaskIdentity;
  /** Root partial or row wire bytes when this task finished successfully. */
  result?: Uint8Array | null;
  failed?: boolean;
  /** Set when any upstream byte was consumed before a producer failure. */
  dirty?: boolean;
};

export function completionAccepted(
  stored: TaskCompletionRecord | undefined,
  identity: TaskIdentity,
): boolean {
  if (!stored) return true;
  if (stored.identity.attempt !== identity.attempt) return identity.attempt > stored.identity.attempt;
  return stored.identity.taskId === identity.taskId && stored.identity.queryId === identity.queryId;
}
