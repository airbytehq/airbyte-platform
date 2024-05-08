import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import {
  getEnforcedDelivery,
  getEnforcedIncrementOrRefresh,
  pruneUnsupportedModes,
} from "./SimplifiedSchemaQuestionnaire";

describe("pruneUnsupportedModes", () => {
  const allModes: Array<[SyncMode, DestinationSyncMode]> = [
    [SyncMode.incremental, DestinationSyncMode.append],
    [SyncMode.incremental, DestinationSyncMode.append_dedup],
    [SyncMode.full_refresh, DestinationSyncMode.append],
    [SyncMode.full_refresh, DestinationSyncMode.overwrite],
  ];

  it("returns all modes when they are supported", () => {
    expect(
      pruneUnsupportedModes(
        allModes,
        [SyncMode.incremental, SyncMode.full_refresh],
        [DestinationSyncMode.append, DestinationSyncMode.append_dedup, DestinationSyncMode.overwrite]
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

describe("getEnforcedDelivery", () => {
  it("selects appendChanges when there are no replicateSource options", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [],
        appendChanges: [
          [SyncMode.incremental, DestinationSyncMode.append],
          [SyncMode.full_refresh, DestinationSyncMode.append],
        ],
      })
    ).toEqual("appendChanges");
  });

  it("selects nothing when both replicateSource and appendChanges are empty", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [],
        appendChanges: [],
      })
    ).toEqual(undefined);
  });

  it("selects replicateSource when there are no appendChanges choices", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [
          [SyncMode.incremental, DestinationSyncMode.append_dedup],
          [SyncMode.full_refresh, DestinationSyncMode.overwrite],
          [SyncMode.incremental, DestinationSyncMode.append],
        ],
        appendChanges: [],
      })
    ).toEqual("replicateSource");
  });

  it("selects the singular option when both replicateSource and appendChanges provide one option", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [[SyncMode.incremental, DestinationSyncMode.append]],
        appendChanges: [[SyncMode.incremental, DestinationSyncMode.append]],
      })
    ).toEqual("replicateSource");
  });

  it("selects nothing when both replicateSource and appendChanges each have one option but they differ", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [[SyncMode.full_refresh, DestinationSyncMode.overwrite]],
        appendChanges: [[SyncMode.full_refresh, DestinationSyncMode.append]],
      })
    ).toEqual(undefined);
  });

  it("selects nothing when there are decisions to be made", () => {
    expect(
      getEnforcedDelivery({
        replicateSource: [
          [SyncMode.incremental, DestinationSyncMode.append_dedup],
          [SyncMode.full_refresh, DestinationSyncMode.overwrite],
          [SyncMode.incremental, DestinationSyncMode.append],
        ],
        appendChanges: [
          [SyncMode.incremental, DestinationSyncMode.append],
          [SyncMode.full_refresh, DestinationSyncMode.append],
        ],
      })
    ).toEqual(undefined);

    expect(
      getEnforcedDelivery({
        replicateSource: [
          [SyncMode.incremental, DestinationSyncMode.append_dedup],
          [SyncMode.full_refresh, DestinationSyncMode.overwrite],
          [SyncMode.incremental, DestinationSyncMode.append],
        ],
        appendChanges: [[SyncMode.incremental, DestinationSyncMode.append]],
      })
    ).toEqual(undefined);

    expect(
      getEnforcedDelivery({
        replicateSource: [[SyncMode.incremental, DestinationSyncMode.append_dedup]],
        appendChanges: [
          [SyncMode.incremental, DestinationSyncMode.append],
          [SyncMode.full_refresh, DestinationSyncMode.append],
        ],
      })
    ).toEqual(undefined);
  });
});

describe("getEnforcedIncrementOrRefresh", () => {
  it("doesn't enforce when there are decisions to be made", () => {
    expect(getEnforcedIncrementOrRefresh([SyncMode.full_refresh, SyncMode.incremental])).toEqual(undefined);
  });

  it("selects the sole option", () => {
    expect(getEnforcedIncrementOrRefresh([SyncMode.full_refresh])).toEqual(SyncMode.full_refresh);
    expect(getEnforcedIncrementOrRefresh([SyncMode.incremental])).toEqual(SyncMode.incremental);
  });
});
