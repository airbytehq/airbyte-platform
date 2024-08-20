import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { pruneUnsupportedModes } from "./preferredSyncModes";

describe("pruneUnsupportedModes", () => {
  const allModes: Array<[SyncMode, DestinationSyncMode]> = [
    [SyncMode.incremental, DestinationSyncMode.append],
    [SyncMode.incremental, DestinationSyncMode.append_dedup],
    [SyncMode.full_refresh, DestinationSyncMode.append],
    [SyncMode.full_refresh, DestinationSyncMode.overwrite],
    [SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup],
  ];

  it("returns all modes when they are supported", () => {
    expect(
      pruneUnsupportedModes(
        allModes,
        [SyncMode.incremental, SyncMode.full_refresh],
        [
          DestinationSyncMode.append,
          DestinationSyncMode.append_dedup,
          DestinationSyncMode.overwrite,
          DestinationSyncMode.overwrite_dedup,
        ]
      )
    ).toEqual(allModes);
  });

  it("filters out unsupported modes", () => {
    expect(pruneUnsupportedModes(allModes, [SyncMode.incremental], [DestinationSyncMode.append])).toEqual([
      [SyncMode.incremental, DestinationSyncMode.append],
    ]);
  });

  it("can return no modes", () => {
    expect(pruneUnsupportedModes(allModes, [SyncMode.full_refresh], [DestinationSyncMode.append_dedup])).toEqual([]);
  });
});
