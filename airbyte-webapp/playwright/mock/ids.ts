/**
 * Deterministic, UUID-shaped IDs. The first block names the kind of entity, the last block is the 1-based
 * index in creation order. Example: ids.connection(2) = "c0000000-0000-4000-8000-000000000002".
 *
 * The IDs match the `[a-f0-9-]{36}` patterns that existing URL assertions use, and they stay the same on
 * every run, so traces and failure messages are comparable across runs.
 *
 * Connector definition IDs are not generated. Definitions use the real IDs from
 * src/area/connector/utils/sources.json and destinations.json, because the webapp changes its behavior
 * based on them.
 */
const prefixes = {
  organization: "0a000000",
  workspace: "a0000000",
  user: "b0000000",
  source: "50000000",
  destination: "d0000000",
  connection: "c0000000",
  catalog: "ca000000",
  event: "e0000000",
  builderProject: "bb000000",
} as const;

export type IdKind = keyof typeof prefixes;

export const ids = Object.fromEntries(
  Object.entries(prefixes).map(([kind, prefix]) => [
    kind,
    (n: number) => `${prefix}-0000-4000-8000-${String(n).padStart(12, "0")}`,
  ])
) as Record<IdKind, (n: number) => string>;
