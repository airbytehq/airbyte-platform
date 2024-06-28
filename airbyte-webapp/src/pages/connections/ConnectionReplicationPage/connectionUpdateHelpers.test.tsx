import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { determineRecommendRefresh } from "./connectionUpdateHelpers";

// guarantees we have the required properties for the functions called
interface RequiredPartialStream {
  stream: { name: string; namespace: string };
  config: {
    syncMode: SyncMode;
    aliasName: string;
    destinationSyncMode: DestinationSyncMode;
    selectedFields: Array<{ fieldPath: string[] }>;
    primaryKey: string[][];
    cursorField: string[];
    selected: boolean;
  };
}

const stream1: RequiredPartialStream = {
  stream: { name: "exampleStream1", namespace: "exampleNamespace" },
  config: {
    aliasName: "exampleStream1",
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.overwrite,
    selectedFields: [{ fieldPath: ["field1"] }],
    primaryKey: [["key1"]],
    cursorField: ["field1"],
    selected: true,
  },
};

const stream2: RequiredPartialStream = {
  stream: { name: "exampleStream2", namespace: "exampleNamespace" },
  config: {
    aliasName: "exampleStream2",
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.append,
    selectedFields: [{ fieldPath: ["field1"] }],
    primaryKey: [["key1"]],
    cursorField: ["field1"],
    selected: true,
  },
};

const stream3: RequiredPartialStream = {
  stream: { name: "exampleStream3", namespace: "exampleNamespace" },
  config: {
    aliasName: "exampleStream3",
    syncMode: SyncMode.incremental,
    destinationSyncMode: DestinationSyncMode.append,
    selectedFields: [{ fieldPath: ["field1"] }],
    primaryKey: [["key1"]],
    cursorField: ["field1"],
    selected: true,
  },
};

const stream4: RequiredPartialStream = {
  stream: { name: "exampleStream4", namespace: "exampleNamespace" },
  config: {
    aliasName: "exampleStream4",
    syncMode: SyncMode.incremental,
    destinationSyncMode: DestinationSyncMode.append_dedup,
    selectedFields: [{ fieldPath: ["field1"] }, { fieldPath: ["field2"] }],
    primaryKey: [["key2"]],
    cursorField: ["field2"],
    selected: true,
  },
};

describe("#determineRecommendRefresh", () => {
  const storedSyncCatalog = {
    streams: [stream1, stream2, stream3, stream4],
  };

  describe("change sync mode to incremental", () => {
    it("Only recommend if full refresh | overwrite changing to incremental | append", () => {
      const updateStream = (stream: RequiredPartialStream) => {
        return {
          ...stream,
          config: {
            ...stream.config,
            syncMode: SyncMode.incremental,
            destinationSyncMode: DestinationSyncMode.append,
          },
        };
      };

      const formSyncCatalog = {
        streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
      };

      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(true);
    });
    it("Only recommend if full refresh | overwrite changing to incremental | append_dedupe", () => {
      const updateStream = (stream: RequiredPartialStream) => {
        return {
          ...stream,
          config: {
            ...stream.config,
            syncMode: SyncMode.incremental,
            destinationSyncMode: DestinationSyncMode.append_dedup,
          },
        };
      };

      const formSyncCatalog = {
        streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
      };

      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(true);
    });
  });
  describe("change sync mode to full refresh", () => {
    it("Does not recommend a refresh when changing to full refresh | overwrite", () => {
      const updateStream = (stream: RequiredPartialStream) => {
        return {
          ...stream,
          config: {
            ...stream.config,
            syncMode: SyncMode.full_refresh,
            destinationSyncMode: DestinationSyncMode.overwrite,
          },
        };
      };

      const formSyncCatalog = {
        streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
      };

      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(false);
    });
    it("Does not recommend a refresh when changing to full refresh | append", () => {
      const updateStream = (stream: RequiredPartialStream) => {
        return {
          ...stream,
          config: {
            ...stream.config,
            syncMode: SyncMode.full_refresh,
            destinationSyncMode: DestinationSyncMode.append,
          },
        };
      };

      const formSyncCatalog = {
        streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
      };

      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(false);
    });
  });

  it("change in primary key suggests refresh if incremental", () => {
    const updateStream = (stream: RequiredPartialStream) => {
      return {
        ...stream,
        config: {
          ...stream.config,
          primaryKey: [["newKey"]],
        },
      };
    };

    const formSyncCatalog = {
      streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
    };
    const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
    expect(result).toBe(true);
  });

  it("change in cursor field suggests refresh if incremental", () => {
    const updateStream = (stream: RequiredPartialStream) => {
      return {
        ...stream,
        config: {
          ...stream.config,
          cursorField: ["newCursorField"],
        },
      };
    };

    const formSyncCatalog = {
      streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
    };
    const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
    expect(result).toBe(true);
  });

  it("changes in selected fields suggest refresh if incremental", () => {
    const updateStream = (stream: RequiredPartialStream) => {
      return {
        ...stream,
        config: {
          ...stream.config,
          selectedFields: [{ fieldPath: ["newField1"] }, { fieldPath: ["newField2"] }],
        },
      };
    };

    const formSyncCatalog = {
      streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
    };
    const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
    expect(result).toBe(true);
  });
  it("changes in stream prefix do not suggest refresh", () => {
    const updateStream = (stream: RequiredPartialStream) => {
      return {
        ...stream,
        config: {
          ...stream.config,
          aliasName: `new${stream.stream.name}`,
        },
      };
    };

    const formSyncCatalog = {
      streams: [updateStream(stream1), updateStream(stream2), updateStream(stream3), updateStream(stream4)],
    };
    const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
    expect(result).toBe(false);
  });
});
