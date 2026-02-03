import { Row } from "@tanstack/react-table";

import { SyncStreamFieldWithId, FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { SyncMode, DestinationSyncMode, AirbyteStreamAndConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import { getStreamChangeStatus, isStreamRow, getSyncCatalogRows, getNamespaceGroups } from "./streamUtils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

const FIELD_ONE: SyncSchemaField = {
  path: ["field_one"],
  cleanedName: "field_one",
  key: "field_one",
  type: "string",
};

const mockedInitialStream: AirbyteStreamAndConfiguration = {
  stream: { name: "stream1", namespace: "namespace1" },
  config: { selected: true, destinationSyncMode: DestinationSyncMode.append_dedup, syncMode: SyncMode.incremental },
};

const mockedStreamNode: SyncStreamFieldWithId = {
  id: "1",
  ...mockedInitialStream,
};

/**
 * getStreamChangeStatus function tests
 */
describe(`${getStreamChangeStatus.name}`, () => {
  it("should return 'unchanged' status", () => {
    const result = getStreamChangeStatus(mockedInitialStream, mockedStreamNode);
    expect(result).toEqual("unchanged");
  });

  it("should return 'disabled' status for a row that initially was not enabled", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      {
        ...mockedStreamNode,
        config: { ...mockedStreamNode.config!, selected: false },
      }
    );
    expect(result).toEqual("disabled");
  });

  it("should return 'added' status for a row that initially was disabled", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      mockedStreamNode
    );
    expect(result).toEqual("added");
  });

  it("should return 'removed' status for a row that initially was enabled", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, selected: false },
    });
    expect(result).toEqual("removed");
  });

  it("should return 'updated' status for a row that has changed 'syncMode' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, syncMode: SyncMode.full_refresh },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'destinationSyncMode' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, destinationSyncMode: DestinationSyncMode.append },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'cursorField' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, cursorField: ["create_time"] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'primaryKey' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, primaryKey: [["id"]] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'selectedFields' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, selectedFields: [{ fieldPath: FIELD_ONE.path }] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'fieldSelectionEnabled' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, fieldSelectionEnabled: false },
    });
    expect(result).toEqual("changed");
  });

  it("should return added styles for a row that is both added and updated", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      {
        ...mockedStreamNode,
        config: { ...mockedStreamNode.config!, syncMode: SyncMode.full_refresh }, // selected true, new sync mode
      }
    );
    expect(result).toEqual("added");
  });
});

/**
 * isStreamRow function tests
 */
describe(`${isStreamRow.name}`, () => {
  it("should return true for a row with depth 1 and rowType 'stream'", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 1,
      original: { rowType: "stream" },
      id: "1-1",
    } as Row<SyncCatalogUIModel>;

    expect(isStreamRow(row)).toBe(true);
  });

  it("should return false for a row with depth 0 and rowType 'stream'", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 0,
      original: { rowType: "stream" },
      id: "0-1",
    } as Row<SyncCatalogUIModel>;

    expect(isStreamRow(row)).toBe(false);
  });

  it("should return false for a row with depth 1 and rowType 'namespace'", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 1,
      original: { rowType: "namespace" },
      id: "1-2",
    } as Row<SyncCatalogUIModel>;

    expect(isStreamRow(row)).toBe(false);
  });

  it("should return false for a row with depth 2 and rowType 'stream'", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 2,
      original: { rowType: "stream" },
      id: "2-1",
    } as Row<SyncCatalogUIModel>;

    expect(isStreamRow(row)).toBe(false);
  });
});

/**
 * groupStreamsByNamespace function tests
 */
describe(`${getNamespaceGroups.name}`, () => {
  it("should group streams by namespace", () => {
    const streams: SyncStreamFieldWithId[] = [
      { stream: { namespace: "namespace1", name: "stream1" }, id: "1" },
      { stream: { namespace: "namespace2", name: "stream2" }, id: "2" },
      { stream: { namespace: "namespace1", name: "stream3" }, id: "3" },
      { stream: { namespace: undefined, name: "stream4" }, id: "4" },
    ];

    const grouped = getNamespaceGroups(streams);

    expect(grouped).toEqual({
      namespace1: [
        { stream: { namespace: "namespace1", name: "stream1" }, id: "1" },
        { stream: { namespace: "namespace1", name: "stream3" }, id: "3" },
      ],
      namespace2: [{ stream: { namespace: "namespace2", name: "stream2" }, id: "2" }],
      "": [{ stream: { namespace: undefined, name: "stream4" }, id: "4" }],
    });
  });

  it("should handle empty streams array", () => {
    const streams: SyncStreamFieldWithId[] = [];

    const grouped = getNamespaceGroups(streams);

    expect(grouped).toEqual({});
  });

  it("should handle streams with only undefined namespaces", () => {
    const streams: SyncStreamFieldWithId[] = [
      { stream: { namespace: undefined, name: "stream1" }, id: "1" },
      { stream: { namespace: undefined, name: "stream2" }, id: "2" },
    ];

    const grouped = getNamespaceGroups(streams);

    expect(grouped).toEqual({
      "": [
        { stream: { namespace: undefined, name: "stream1" }, id: "1" },
        { stream: { name: "stream2" }, id: "2" },
      ],
    });
  });
});

/**
 * getSyncCatalogRows function tests
 */
describe(`${getSyncCatalogRows.name}`, () => {
  const streams: SyncStreamFieldWithId[] = [
    {
      stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
      config: { selected: true, destinationSyncMode: DestinationSyncMode.append_dedup, syncMode: SyncMode.incremental },
      id: "1",
    },
    {
      stream: { name: "stream2", namespace: "namespace1", jsonSchema: {} },
      config: {
        selected: false,
        destinationSyncMode: DestinationSyncMode.append_dedup,
        syncMode: SyncMode.incremental,
      },
      id: "2",
    },
  ];

  const initialStreams: FormConnectionFormValues["syncCatalog"]["streams"] = [
    {
      stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
      config: { selected: true, destinationSyncMode: DestinationSyncMode.append_dedup, syncMode: SyncMode.incremental },
    },
    {
      stream: { name: "stream2", namespace: "namespace1", jsonSchema: {} },
      config: {
        selected: false,
        destinationSyncMode: DestinationSyncMode.append_dedup,
        syncMode: SyncMode.incremental,
      },
    },
  ];

  it("should group streams by namespace and prepare rows", () => {
    const result = getSyncCatalogRows(streams, initialStreams);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("namespace1");
    expect(result[0].subRows).toHaveLength(2);
    expect(result[0].subRows[0].name).toBe("stream1");
    expect(result[0].subRows[1].name).toBe("stream2");
  });

  it("should handle prefix correctly", () => {
    const prefix = "prefix-";
    const result = getSyncCatalogRows(streams, initialStreams, prefix);
    expect(result[0].subRows[0].name).toBe("prefix-stream1");
    expect(result[0].subRows[1].name).toBe("prefix-stream2");
  });

  it("should handle empty streams array", () => {
    const result = getSyncCatalogRows([], []);
    expect(result).toHaveLength(0);
  });

  it("should handle streams with different namespaces", () => {
    const streamsWithDifferentNamespaces: SyncStreamFieldWithId[] = [
      {
        stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "1",
      },
      {
        stream: { name: "stream2", namespace: "namespace2", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "2",
      },
    ];

    const initialStreamsWithDifferentNamespaces: FormConnectionFormValues["syncCatalog"]["streams"] = [
      {
        stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
      {
        stream: { name: "stream2", namespace: "namespace2", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
    ];

    const result = getSyncCatalogRows(streamsWithDifferentNamespaces, initialStreamsWithDifferentNamespaces);
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("namespace1");
    expect(result[0].subRows).toHaveLength(1);
    expect(result[0].subRows[0].name).toBe("stream1");
    expect(result[1].name).toBe("namespace2");
    expect(result[1].subRows).toHaveLength(1);
    expect(result[1].subRows[0].name).toBe("stream2");
  });

  it("should handle streams with no namespace", () => {
    const streamsNoNamespace: SyncStreamFieldWithId[] = [
      {
        stream: { name: "stream1", namespace: "", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "1",
      },
      {
        stream: { name: "stream2", namespace: "", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "2",
      },
    ];

    const initialStreamsNoNamespace: FormConnectionFormValues["syncCatalog"]["streams"] = [
      {
        stream: { name: "stream1", namespace: "", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
      {
        stream: { name: "stream2", namespace: "", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
    ];

    const result = getSyncCatalogRows(streamsNoNamespace, initialStreamsNoNamespace);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("");
    expect(result[0].subRows).toHaveLength(2);
    expect(result[0].subRows[0].name).toBe("stream1");
    expect(result[0].subRows[1].name).toBe("stream2");
  });

  it("should handle streams with mixed namespaces and no namespace", () => {
    const mixedStreams: SyncStreamFieldWithId[] = [
      {
        stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "1",
      },
      {
        stream: { name: "stream2", namespace: "", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
        id: "2",
      },
    ];

    const initialMixedStreams: FormConnectionFormValues["syncCatalog"]["streams"] = [
      {
        stream: { name: "stream1", namespace: "namespace1", jsonSchema: {} },
        config: {
          selected: true,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
      {
        stream: { name: "stream2", namespace: "", jsonSchema: {} },
        config: {
          selected: false,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          syncMode: SyncMode.incremental,
        },
      },
    ];

    const result = getSyncCatalogRows(mixedStreams, initialMixedStreams);
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("namespace1");
    expect(result[0].subRows).toHaveLength(1);
    expect(result[0].subRows[0].name).toBe("stream1");
    expect(result[1].name).toBe("");
    expect(result[1].subRows).toHaveLength(1);
    expect(result[1].subRows[0].name).toBe("stream2");
  });
});
