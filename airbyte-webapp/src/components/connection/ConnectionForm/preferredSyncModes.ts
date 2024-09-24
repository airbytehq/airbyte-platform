import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

export const pruneUnsupportedModes = (
  modes: Array<[SyncMode, DestinationSyncMode]>,
  supportedSyncModes: SyncMode[],
  supportedDestinationSyncModes: DestinationSyncMode[] | undefined
) => {
  return modes.filter(([syncMode, destinationSyncMode]) => {
    return supportedSyncModes.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode);
  });
};

export const replicateSourceModes: Array<[SyncMode, DestinationSyncMode]> = [
  [SyncMode.incremental, DestinationSyncMode.append_dedup],
  [SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup],
  [SyncMode.full_refresh, DestinationSyncMode.overwrite],
  [SyncMode.incremental, DestinationSyncMode.append],
];

export const appendChangesModes: Array<[SyncMode, DestinationSyncMode]> = [
  [SyncMode.incremental, DestinationSyncMode.append],
  [SyncMode.full_refresh, DestinationSyncMode.append],
];
