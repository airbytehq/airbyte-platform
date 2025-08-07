import { mockSourceDiscoverSchemaRead } from "test-utils/mock-data/mockSource";

import { DestinationSyncMode, SyncMode, StreamMapperType } from "core/api/types/AirbyteClient";
import { FilterCondition } from "pages/connections/ConnectionMappingsPage/RowFilteringMapperForm";

import { createSyncCatalogFromFormValues } from "./createSyncCatalogFromFormValues";
import { DataActivationConnectionFormOutput } from "./DataActivationConnectionFormSchema";

const MOCK_RSA_PUBLIC_KEY =
  "30820122300d06092a864886f70d01010105000382010f003082010a0282010100cabb23666f3c430417a9b6455c246189c282248fad5acc5c405d20e97c0c2dde125e16a5baa249e3400f286d7142e94d92d0a0d2bffaa6c3dfa6b87cc24fe93125ac5f8646cb37ef823732aa5bdc0ee2770f091c1a0c51ad7e01744133cb43604898ac452e90fd3457436889de6d6f14773154f35a0ee7e22b9432fe3afec52736a0c6a2702b4251ace0d8e2b805ae0714703b8db5ddeff297c7b16e56f4c15709b5140c2415c23c850d38b989317037df753fa89321c73b440c9425ccebf3520b121b135750bb727d998157f29f9c93939b39656cf95782938786d66e9e6be99ccf6f55af055889fe601ade7066d9a7ea04ee7ef266693954e9f216568f0e5f0203010001";

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
          fields: [
            { sourceFieldName: "name", destinationFieldName: "dest_name", additionalMappers: [] },
            { sourceFieldName: "id", destinationFieldName: "dest_id", additionalMappers: [] },
          ],
          cursorField: "created_at",
          matchingKeys: ["dest_id"],
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
                type: "field-filtering",
                mapperConfiguration: {
                  targetField: "created_at",
                },
              },
              {
                type: "field-renaming",
                mapperConfiguration: {
                  newFieldName: "dest_name",
                  originalFieldName: "name",
                },
              },
              {
                type: "field-renaming",
                mapperConfiguration: {
                  newFieldName: "dest_id",
                  originalFieldName: "id",
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

  it("correctly creates a field hashing mapper from form values", () => {
    if (!mockSourceDiscoverSchemaRead.catalog) {
      throw new Error("This test relies on the mock source discover catalog being defined");
    }

    const formValuesWithHashingMapper: DataActivationConnectionFormOutput = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: "test_namespace",
          },
          destinationObjectName: "destination_stream",
          sourceSyncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          fields: [
            {
              sourceFieldName: "source_field",
              destinationFieldName: "destination_field",
              additionalMappers: [
                {
                  type: StreamMapperType.hashing,
                  method: "MD5",
                },
              ],
            },
          ],
          cursorField: null,
          matchingKeys: null,
        },
      ],
    };

    const result = createSyncCatalogFromFormValues(formValuesWithHashingMapper, mockSourceDiscoverSchemaRead.catalog);

    expect(result.streams[0].config?.mappers).toEqual([
      {
        type: StreamMapperType.hashing,
        mapperConfiguration: {
          targetField: "source_field",
          method: "MD5",
          fieldNameSuffix: "",
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          originalFieldName: "source_field",
          newFieldName: "destination_field",
        },
      },
    ]);
  });

  it("correctly creates row filtering mappers from form values", () => {
    if (!mockSourceDiscoverSchemaRead.catalog) {
      throw new Error("This test relies on the mock source discover catalog being defined");
    }

    const formValuesWithRowFilteringMappers: DataActivationConnectionFormOutput = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: "test_namespace",
          },
          destinationObjectName: "destination_stream",
          sourceSyncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          fields: [
            {
              sourceFieldName: "source_field",
              destinationFieldName: "destination_field",
              additionalMappers: [
                {
                  type: StreamMapperType["row-filtering"],
                  condition: FilterCondition.IN,
                  comparisonValue: "some value",
                },
              ],
            },
            {
              sourceFieldName: "source_field_2",
              destinationFieldName: "destination_field_2",
              additionalMappers: [
                {
                  type: StreamMapperType["row-filtering"],
                  condition: FilterCondition.OUT,
                  comparisonValue: "some other value",
                },
              ],
            },
          ],
          cursorField: null,
          matchingKeys: null,
        },
      ],
    };

    const result = createSyncCatalogFromFormValues(
      formValuesWithRowFilteringMappers,
      mockSourceDiscoverSchemaRead.catalog
    );

    expect(result.streams[0].config?.mappers).toEqual([
      {
        type: StreamMapperType["row-filtering"],
        mapperConfiguration: {
          conditions: {
            type: "EQUAL",
            fieldName: "source_field",
            comparisonValue: "some value",
          },
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          originalFieldName: "source_field",
          newFieldName: "destination_field",
        },
      },
      {
        type: StreamMapperType["row-filtering"],
        mapperConfiguration: {
          conditions: {
            type: "NOT",
            conditions: [
              {
                type: "EQUAL",
                fieldName: "source_field_2",
                comparisonValue: "some other value",
              },
            ],
          },
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          originalFieldName: "source_field_2",
          newFieldName: "destination_field_2",
        },
      },
    ]);
  });

  it("correctly creates an encryption mapper from form values", () => {
    if (!mockSourceDiscoverSchemaRead.catalog) {
      throw new Error("This test relies on the mock source discover catalog being defined");
    }

    const formValuesWithEncryptionMapper: DataActivationConnectionFormOutput = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: "test_namespace",
          },
          destinationObjectName: "destination_stream",
          sourceSyncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          fields: [
            {
              sourceFieldName: "source_field",
              destinationFieldName: "destination_field",
              additionalMappers: [
                {
                  type: StreamMapperType.encryption,
                  publicKey: MOCK_RSA_PUBLIC_KEY,
                },
              ],
            },
          ],
          cursorField: null,
          matchingKeys: null,
        },
      ],
    };

    const result = createSyncCatalogFromFormValues(
      formValuesWithEncryptionMapper,
      mockSourceDiscoverSchemaRead.catalog
    );

    expect(result.streams[0].config?.mappers).toEqual([
      {
        type: StreamMapperType.encryption,
        mapperConfiguration: {
          algorithm: "RSA",
          targetField: "source_field",
          publicKey: MOCK_RSA_PUBLIC_KEY,
          fieldNameSuffix: "",
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          originalFieldName: "source_field",
          newFieldName: "destination_field",
        },
      },
    ]);
  });

  it("should add field filtering mapper for cursor field when cursor is not mapped to destination field", () => {
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
          fields: [
            { sourceFieldName: "name", destinationFieldName: "dest_name", additionalMappers: [] },
            { sourceFieldName: "id", destinationFieldName: "dest_id", additionalMappers: [] },
          ],
          cursorField: "created_at",
          matchingKeys: ["dest_id"],
        },
      ],
    };
    const result = createSyncCatalogFromFormValues(mappedStreams, mockSourceDiscoverSchemaRead.catalog);

    const mappers = result.streams[0].config?.mappers;
    expect(mappers).toHaveLength(3);

    // Should have field filtering mapper for cursor field
    expect(mappers?.[0]).toEqual({
      type: StreamMapperType["field-filtering"],
      mapperConfiguration: {
        targetField: "created_at",
      },
    });

    // Should have field renaming mappers for the other fields
    expect(mappers?.[1]).toEqual({
      type: StreamMapperType["field-renaming"],
      mapperConfiguration: {
        newFieldName: "dest_name",
        originalFieldName: "name",
      },
    });

    expect(mappers?.[2]).toEqual({
      type: StreamMapperType["field-renaming"],
      mapperConfiguration: {
        newFieldName: "dest_id",
        originalFieldName: "id",
      },
    });
  });

  it("should NOT add field filtering mapper for cursor field when cursor IS mapped to destination field", () => {
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
          fields: [
            { sourceFieldName: "name", destinationFieldName: "dest_name", additionalMappers: [] },
            { sourceFieldName: "id", destinationFieldName: "dest_id", additionalMappers: [] },
            { sourceFieldName: "created_at", destinationFieldName: "dest_created_at", additionalMappers: [] },
          ],
          cursorField: "created_at",
          matchingKeys: ["dest_id"],
        },
      ],
    };
    const result = createSyncCatalogFromFormValues(mappedStreams, mockSourceDiscoverSchemaRead.catalog);

    const selectedFields = result.streams[0].config?.selectedFields;
    expect(selectedFields).toHaveLength(3);
    expect(selectedFields).toEqual([{ fieldPath: ["name"] }, { fieldPath: ["id"] }, { fieldPath: ["created_at"] }]);

    const mappers = result.streams[0].config?.mappers;
    expect(mappers).toHaveLength(3);

    // Should NOT have field filtering mapper for cursor field since it's mapped
    expect(mappers?.every((mapper) => mapper.type !== StreamMapperType["field-filtering"])).toBe(true);

    // Should have field renaming mappers for all fields including cursor
    expect(mappers).toEqual([
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          newFieldName: "dest_name",
          originalFieldName: "name",
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          newFieldName: "dest_id",
          originalFieldName: "id",
        },
      },
      {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          newFieldName: "dest_created_at",
          originalFieldName: "created_at",
        },
      },
    ]);
  });
});
