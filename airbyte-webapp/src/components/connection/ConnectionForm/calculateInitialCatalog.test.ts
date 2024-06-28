import {
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  DestinationSyncMode,
  FieldTransformTransformType,
  SchemaChange,
  StreamTransformTransformType,
  SyncMode,
} from "core/api/types/AirbyteClient";

import { analyzeSyncCatalogBreakingChanges } from "./calculateInitialCatalog";

const mockSyncSchemaStream: AirbyteStreamAndConfiguration = {
  stream: {
    sourceDefinedCursor: true,
    defaultCursorField: ["new_source_cursor"],
    sourceDefinedPrimaryKey: [["new_primary_key"]],
    jsonSchema: {},
    name: "test",
    namespace: "namespace-test",
    supportedSyncModes: [],
  },
  config: {
    destinationSyncMode: DestinationSyncMode.append,
    selected: false,
    syncMode: SyncMode.full_refresh,
    cursorField: ["old_cursor"],
    primaryKey: [["old_primary_key"]],
    aliasName: "",
  },
};

const mockSyncSchemaStreamUserDefined: AirbyteStreamAndConfiguration = {
  stream: {
    sourceDefinedCursor: true,
    defaultCursorField: [],
    sourceDefinedPrimaryKey: [],
    jsonSchema: {},
    name: "test",
    namespace: "namespace-test",
    supportedSyncModes: [],
  },
  config: {
    destinationSyncMode: DestinationSyncMode.append,
    selected: false,
    syncMode: SyncMode.full_refresh,
    cursorField: ["old_cursor"],
    primaryKey: [["old_primary_key"]],
    aliasName: "",
  },
};

describe("analyzeSyncCatalogBreakingChanges", () => {
  it("should return syncCatalog unchanged when schemaChange is no_change and catalogDiff is undefined", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStream] };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, undefined, SchemaChange.no_change);
    expect(result).toEqual(syncCatalog);
  });

  it("should return syncCatalog unchanged when schemaChange is non_breaking and catalogDiff is undefined", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStream] };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, undefined, SchemaChange.non_breaking);
    expect(result).toEqual(syncCatalog);
  });

  it("should return syncCatalog with transformed streams when there are breaking changes - primaryKey", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStream] };
    const catalogDiff: CatalogDiff = {
      transforms: [
        {
          transformType: StreamTransformTransformType.update_stream,
          streamDescriptor: { name: "test", namespace: "namespace-test" },
          updateStream: {
            streamAttributeTransforms: [],
            fieldTransforms: [
              {
                breaking: true,
                transformType: FieldTransformTransformType.remove_field,
                fieldName: ["old_primary_key"],
              },
            ],
          },
        },
      ],
    };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, SchemaChange.breaking);
    expect(result.streams[0].config?.primaryKey).toEqual([["new_primary_key"]]);
  });

  it("should return syncCatalog with transformed streams when there are breaking changes - cursor", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStream] };
    const catalogDiff: CatalogDiff = {
      transforms: [
        {
          transformType: StreamTransformTransformType.update_stream,
          streamDescriptor: { name: "test", namespace: "namespace-test" },
          updateStream: {
            streamAttributeTransforms: [],
            fieldTransforms: [
              {
                breaking: true,
                transformType: FieldTransformTransformType.remove_field,
                fieldName: ["old_cursor"],
              },
            ],
          },
        },
      ],
    };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, SchemaChange.breaking);
    expect(result.streams[0].config?.cursorField).toEqual(["new_source_cursor"]);
  });

  it("should return syncCatalog with transformed streams when there are breaking changes - primaryKey - user-defined", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStreamUserDefined] };
    const catalogDiff: CatalogDiff = {
      transforms: [
        {
          transformType: StreamTransformTransformType.update_stream,
          streamDescriptor: { name: "test", namespace: "namespace-test" },
          updateStream: {
            streamAttributeTransforms: [],
            fieldTransforms: [
              {
                breaking: true,
                transformType: FieldTransformTransformType.remove_field,
                fieldName: ["old_primary_key"],
              },
            ],
          },
        },
      ],
    };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, SchemaChange.breaking);
    expect(result.streams[0].config?.primaryKey).toEqual([]);
  });

  it("should return syncCatalog with transformed streams when there are breaking changes - cursor - user-defined", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStreamUserDefined] };
    const catalogDiff: CatalogDiff = {
      transforms: [
        {
          transformType: StreamTransformTransformType.update_stream,
          streamDescriptor: { name: "test", namespace: "namespace-test" },
          updateStream: {
            streamAttributeTransforms: [],
            fieldTransforms: [
              {
                breaking: true,
                transformType: FieldTransformTransformType.remove_field,
                fieldName: ["old_cursor"],
              },
            ],
          },
        },
      ],
    };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, SchemaChange.breaking);
    expect(result.streams[0].config?.cursorField).toEqual([]);
  });

  it("should return syncCatalog unchanged when there are no breaking changes", () => {
    const syncCatalog: AirbyteCatalog = { streams: [mockSyncSchemaStream] };
    const catalogDiff: CatalogDiff = {
      transforms: [
        {
          transformType: StreamTransformTransformType.update_stream,
          streamDescriptor: { name: "test", namespace: "namespace-test" },
          updateStream: {
            streamAttributeTransforms: [],
            fieldTransforms: [
              {
                breaking: false,
                transformType: FieldTransformTransformType.add_field,
                fieldName: ["new_field"],
              },
            ],
          },
        },
      ],
    };
    const result = analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, SchemaChange.breaking);
    expect(result).toEqual(syncCatalog);
  });
});
