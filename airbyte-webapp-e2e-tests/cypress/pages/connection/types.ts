import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";

export const SYNC_MODE_STRINGS: Readonly<Record<SourceSyncMode | DestinationSyncMode, string>> = {
  [SourceSyncMode.FullRefresh]: "Full refresh",
  [SourceSyncMode.Incremental]: "Incremental",
  [DestinationSyncMode.Append]: "Append",
  [DestinationSyncMode.AppendDedup]: "Deduped + history",
  [DestinationSyncMode.Overwrite]: "Overwrite",
};
