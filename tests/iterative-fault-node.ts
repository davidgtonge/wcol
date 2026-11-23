/** Test-only fault injection; all loading and round execution use the production node. */
import "../cloudflare/src/node/entry.ts";
import { WcolNode as ProductionNode } from "../cloudflare/src/node/node.ts";
import type {
  ResidentKMeansLoadParams,
  ResidentKMeansRoundParams,
} from "../cloudflare/src/node/types.ts";

export class WcolNode extends ProductionNode {
  private loadedSlot: number | undefined;
  async roundError(params: ResidentKMeansRoundParams): Promise<string | null> {
    try {
      await this.runResidentKMeansRound(params);
      return null;
    } catch (error) {
      return String(error);
    }
  }

  async loadError(params: ResidentKMeansLoadParams): Promise<string | null> {
    try {
      await this.loadResidentKMeans(params);
      return null;
    } catch (error) {
      return String(error);
    }
  }

  async loadResidentKMeans(params: ResidentKMeansLoadParams) {
    const result = await super.loadResidentKMeans(params);
    this.loadedSlot = params.slot;
    if (params.jobId === "miniflare-kmeans-recovery") {
      await this.nodeEnv.OUTPUT.put(
        `_faults/${params.jobId}/a${params.executionAttempt}-s${params.slot}-load.json`,
        JSON.stringify({
          startIteration: params.startIteration,
          moments: Array.from(result.moments),
        }),
      );
    }
    return result;
  }

  async runResidentKMeansRound(params: ResidentKMeansRoundParams) {
    if (params.jobId === "miniflare-kmeans-recovery") {
      await this.nodeEnv.OUTPUT.put(
        `_faults/${params.jobId}/a${params.executionAttempt}-${this.nodeState.id}-r${params.iteration}.json`,
        JSON.stringify({
          iteration: params.iteration,
          state: Array.from(params.globalState),
        }),
      );
      if (
        this.loadedSlot === 0 &&
        params.executionAttempt === 1 &&
        params.iteration === 2
      ) {
        // A committed barrier exists. Kill the actual object, losing its Wasm and partition.
        this.nodeState.abort("injected resident node loss after barrier one");
      }
    }
    return super.runResidentKMeansRound(params);
  }
}
