import { mockSourceDiscoverSchemaRead } from "test-utils/mock-data/mockSource";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { createSyncCatalogFromMappedStreams } from "./createSyncCatalogFromMappedStreams";
import { StreamMappingsFormValuesType } from "./StreamMappings";

describe(`${createSyncCatalogFromMappedStreams.name}`, () => {
  it("should select no streams when mapped streams are empty", () => {
    const result = createSyncCatalogFromMappedStreams({ streams: [] }, mockSourceDiscoverSchemaRead);

    expect(result).toEqual({
      streams: [
        {
          config: {
            ...mockSourceDiscoverSchemaRead.catalog?.streams[0].config,
            fieldSelectionEnabled: false,
            mappers: undefined,
            selected: false,
            selectedFields: undefined,
          },
          stream: mockSourceDiscoverSchemaRead.catalog?.streams[0].stream,
        },
      ],
    });
  });

  it("should select appropriate streams and fields given mapped streams", () => {
    const mappedStreams: StreamMappingsFormValuesType = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: "test_namespace",
          },
          destinationObjectName: "destination_stream_1",
          sourceSyncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          fields: [
            { sourceFieldName: "id", destinationFieldName: "dest_id" },
            { sourceFieldName: "name", destinationFieldName: "dest_name" },
          ],
          cursorField: "created_at",
          primaryKey: "id",
        },
      ],
    };

    const result = createSyncCatalogFromMappedStreams(mappedStreams, mockSourceDiscoverSchemaRead);

    expect(result).toEqual({
      streams: [
        {
          config: {
            ...mockSourceDiscoverSchemaRead.catalog?.streams[0].config,
            destinationSyncMode: DestinationSyncMode.append_dedup,
            primaryKey: [["id"]],
            syncMode: SyncMode.incremental,
            cursorField: ["created_at"],
            selected: true,
            fieldSelectionEnabled: true,
            destinationObjectName: "destination_stream_1",
            selectedFields: [
              { fieldPath: ["id"], selected: true },
              { fieldPath: ["name"], selected: true },
            ],
            mappers: [
              {
                id: expect.any(String),
                type: "field-renaming",
                mapperConfiguration: {
                  newFieldName: "dest_id",
                  originalFieldName: "id",
                },
              },
              {
                id: expect.any(String),
                type: "field-renaming",
                mapperConfiguration: {
                  newFieldName: "dest_name",
                  originalFieldName: "name",
                },
              },
            ],
          },
          stream: {
            ...mockSourceDiscoverSchemaRead.catalog?.streams[0].stream,
            name: "test_stream",
          },
        },
      ],
    });
  });
});
