/** Minimum spill runs before a plateau / retention verdict is meaningful. */

export function minSpillRunsForDataset(strict: boolean): number {
  if (strict) return 40;
  return 15;
}

export type SpillCoverageVerdict = "ok" | "insufficient";

export function spillCoverageVerdict(spillRuns: number, strict: boolean): SpillCoverageVerdict {
  const minRuns = minSpillRunsForDataset(strict);
  if (strict && spillRuns < minRuns) return "insufficient";
  return "ok";
}

export function assertSpillCoverageOrThrow(label: string, spillRuns: number, strict: boolean): void {
  const minRuns = minSpillRunsForDataset(strict);
  if (strict && spillRuns < minRuns) {
    throw new Error(
      `${label}: insufficient spill coverage (${spillRuns} runs, need ${minRuns}+); plateau not evaluated`,
    );
  }
}

export function formatSpillCoverageNote(label: string, spillRuns: number, strict: boolean): string | null {
  const minRuns = minSpillRunsForDataset(strict);
  if (spillRuns >= minRuns) return null;
  if (strict) return null;
  return `note: ${label} saw ${spillRuns} runs (< ${minRuns}); plateau skipped`;
}
