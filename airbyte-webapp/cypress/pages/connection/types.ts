import { DestinationSyncMode, SyncMode } from "@src/core/api/types/AirbyteClient";

export const SYNC_MODE_STRINGS: Readonly<Record<SyncMode | DestinationSyncMode, string>> = {
  [SyncMode.full_refresh]: "Full refresh",
  [SyncMode.incremental]: "Incremental",
  [DestinationSyncMode.append]: "Append",
  [DestinationSyncMode.append_dedup]: "Append + Deduped",
  [DestinationSyncMode.overwrite]: "Overwrite",
};
