/**
 * Container instance types for container reducers, and the group budget each allows. Plain
 * data with no runtime imports: the console's page uses it too.
 */

/** Container instance types a container reducer may start on (Cloudflare's published sizes). */
export const CONTAINER_SPECS = {
  lite: { vcpu: 1 / 16, memoryMib: 256, diskGb: 2 },
  basic: { vcpu: 1 / 4, memoryMib: 1024, diskGb: 4 },
  "standard-1": { vcpu: 1 / 2, memoryMib: 4096, diskGb: 8 },
  "standard-2": { vcpu: 1, memoryMib: 6144, diskGb: 12 },
  "standard-3": { vcpu: 2, memoryMib: 8192, diskGb: 16 },
  "standard-4": { vcpu: 4, memoryMib: 12288, diskGb: 20 },
} as const;
export type ContainerInstance = keyof typeof CONTAINER_SPECS;
export const CONTAINER_INSTANCES = Object.keys(CONTAINER_SPECS) as ContainerInstance[];
/** Measured on Q32: 32 basic reducers ran as fast as 32 standard-1 at half the cost. */
export const DEFAULT_CONTAINER_INSTANCE: ContainerInstance = "basic";

/**
 * A container reducer's group-table budget: its memory less 128 MiB for edge buffers and the
 * engine. A table up to half this finishes in memory without spilling; on Q32 a basic
 * reducer's 396 MB table finished in 412 MB resident, since a ranked finish keeps only its top k.
 */
export function containerGroupBytes(instance: ContainerInstance): number {
  return (CONTAINER_SPECS[instance].memoryMib - 128) * 1024 * 1024;
}
