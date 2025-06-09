import { mockSourceDiscoverSchemaRead } from "test-utils/mock-data/mockSource";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { createSyncCatalogFromFormValues } from "./createSyncCatalogFromFormValues";
import { DataActivationConnectionFormOutput } from "./DataActivationConnectionFormSchema";

describe(`${createSyncCatalogFromFormValues.name}`, () => {
  it("should select no streams when mapped streams are empty", () => {
    if (!mockSourceDiscoverSchemaRead.catalog) {
      throw new Error("This test relies on the mock source discover catalog being defined");
    }
    const result = createSyncCatalogFromFormValues({ streams: [] }, mockSourceDiscoverSchemaRead.catalog);

    result.streams.forEach((stream) => {
      expect(stream?.config?.selected).toBe(false);
    });
  });

  it("should select appropriate streams and fields given mapped streams", () => {
    if (!mockSourceDiscoverSchemaRead.catalog) {
      throw new Error("This test relies on the mock source discover catalog being defined");
    }
    const mappedStreams: DataActivationConnectionFormOutput = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: "test_namespace",
          },
          destinationObjectName: "destination_stream_1",
          sourceSyncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          fields: [{ sourceFieldName: "name", destinationFieldName: "dest_name" }],
          cursorField: "created_at",
          primaryKey: "id",
        },
      ],
    };
    const result = createSyncCatalogFromFormValues(mappedStreams, mockSourceDiscoverSchemaRead.catalog);

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
              { fieldPath: ["name"] },
              // The primary key field must be selected
              { fieldPath: ["id"] },
              // The cursor field must be selected
              { fieldPath: ["created_at"] },
            ],
            mappers: [
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
