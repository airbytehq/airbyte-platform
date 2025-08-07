import { AirbyteCatalog, AirbyteStreamConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { createFormDefaultValues, getMatchingKeysFromConfig } from "./createFormDefaultValues";

const MOCK_RSA_PUBLIC_KEY =
  "30820122300d06092a864886f70d01010105000382010f003082010a0282010100cabb23666f3c430417a9b6455c246189c282248fad5acc5c405d20e97c0c2dde125e16a5baa249e3400f286d7142e94d92d0a0d2bffaa6c3dfa6b87cc24fe93125ac5f8646cb37ef823732aa5bdc0ee2770f091c1a0c51ad7e01744133cb43604898ac452e90fd3457436889de6d6f14773154f35a0ee7e22b9432fe3afec52736a0c6a2702b4251ace0d8e2b805ae0714703b8db5ddeff297c7b16e56f4c15709b5140c2415c23c850d38b989317037df753fa89321c73b440c9425ccebf3520b121b135750bb727d998157f29f9c93939b39656cf95782938786d66e9e6be99ccf6f55af055889fe601ade7066d9a7ea04ee7ef266693954e9f216568f0e5f0203010001";

describe(`${createFormDefaultValues.name}`, () => {
  it("should return default values for a given sync catalog", () => {
    const syncCatalog: AirbyteCatalog = {
      streams: [
        {
          stream: { name: "test_stream", namespace: "test_namespace" },
          config: {
            selected: true,
            syncMode: "full_refresh",
            destinationSyncMode: "append",
            primaryKey: undefined,
            cursorField: [],
            mappers: [],
          },
        },
      ],
    };

    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: { name: "test_stream", namespace: "test_namespace" },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
          fields: [],
          matchingKeys: null,
          cursorField: null,
        },
      ],
    };

    expect(createFormDefaultValues(syncCatalog)).toEqual(expectedDefaultValues);
  });

  it("return default values for mock hubspot catalog", () => {
    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "campaigns",
            namespace: undefined,
          },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
          fields: [{ sourceFieldName: "id", destinationFieldName: "destination_id", additionalMappers: [] }],
          matchingKeys: ["destination_id"],
          cursorField: "lastUpdatedTime",
        },
      ],
    };

    expect(createFormDefaultValues(PARTIAL_HUBSPOT_SYNC_CATALOG)).toEqual(expectedDefaultValues);
  });

  it("returns correct composite matching key", () => {
    const SYNC_CATALOG_WITH_COMPOSITE_KEY: AirbyteCatalog = {
      streams: [
        {
          stream: PARTIAL_HUBSPOT_SYNC_CATALOG.streams[0].stream,
          config: {
            ...PARTIAL_HUBSPOT_SYNC_CATALOG.streams[0].config!,
            primaryKey: [["id"], ["name"]],
            mappers: [
              {
                id: "mapper1",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "id",
                  newFieldName: "destination_id",
                },
              },
              {
                id: "mapper2",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "name",
                  newFieldName: "destination_name",
                },
              },
            ],
          },
        },
      ],
    };

    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "campaigns",
            namespace: undefined,
          },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
          fields: [
            { sourceFieldName: "id", destinationFieldName: "destination_id", additionalMappers: [] },
            { sourceFieldName: "name", destinationFieldName: "destination_name", additionalMappers: [] },
          ],
          matchingKeys: ["destination_id", "destination_name"],
          cursorField: "lastUpdatedTime",
        },
      ],
    };

    expect(createFormDefaultValues(SYNC_CATALOG_WITH_COMPOSITE_KEY)).toEqual(expectedDefaultValues);
  });

  it("correctly parses a field hashing mapper", () => {
    const syncCatalogWithHashingMapper: AirbyteCatalog = {
      streams: [
        {
          stream: { name: "test_stream", namespace: undefined },
          config: {
            selected: true,
            syncMode: "full_refresh",
            destinationSyncMode: "append",
            primaryKey: undefined,
            cursorField: [],
            mappers: [
              {
                id: "hashing_mapper",
                type: StreamMapperType.hashing,
                mapperConfiguration: {
                  fieldNameSuffix: "",
                  method: "MD5",
                  targetField: "source_field",
                },
              },
              {
                id: "renaming_mapper",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "source_field",
                  newFieldName: "destination_field",
                },
              },
            ],
          },
        },
      ],
    };

    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: undefined,
          },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
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
          matchingKeys: null,
          cursorField: null,
        },
      ],
    };

    expect(createFormDefaultValues(syncCatalogWithHashingMapper)).toEqual(expectedDefaultValues);
  });

  it("correctly parses row filtering mappers", () => {
    const syncCatalogWithRowFilteringMapper: AirbyteCatalog = {
      streams: [
        {
          stream: { name: "test_stream", namespace: undefined },
          config: {
            selected: true,
            syncMode: "full_refresh",
            destinationSyncMode: "append",
            primaryKey: undefined,
            cursorField: [],
            mappers: [
              {
                id: "row_filtering_mapper",
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
                id: "renaming_mapper",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "source_field",
                  newFieldName: "destination_field",
                },
              },
              {
                id: "row_filtering_mapper",
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
                id: "renaming_mapper",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "source_field_2",
                  newFieldName: "destination_field_2",
                },
              },
            ],
          },
        },
      ],
    };

    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: undefined,
          },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
          fields: [
            {
              sourceFieldName: "source_field",
              destinationFieldName: "destination_field",
              additionalMappers: [
                {
                  type: StreamMapperType["row-filtering"],
                  condition: "IN",
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
                  condition: "OUT",
                  comparisonValue: "some other value",
                },
              ],
            },
          ],
          matchingKeys: null,
          cursorField: null,
        },
      ],
    };

    expect(createFormDefaultValues(syncCatalogWithRowFilteringMapper)).toEqual(expectedDefaultValues);
  });

  it("correctly parses an encryption mapper", () => {
    const syncCatalogWithEncryptionMapper: AirbyteCatalog = {
      streams: [
        {
          stream: { name: "test_stream", namespace: undefined },
          config: {
            selected: true,
            syncMode: "full_refresh",
            destinationSyncMode: "append",
            primaryKey: undefined,
            cursorField: [],
            mappers: [
              {
                id: "encryption_mapper",
                type: StreamMapperType.encryption,
                mapperConfiguration: {
                  algorithm: "RSA",
                  fieldNameSuffix: "",
                  publicKey: MOCK_RSA_PUBLIC_KEY,
                  targetField: "source_field",
                },
              },
              {
                id: "renaming_mapper",
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  originalFieldName: "source_field",
                  newFieldName: "destination_field",
                },
              },
            ],
          },
        },
      ],
    };

    const expectedDefaultValues = {
      streams: [
        {
          sourceStreamDescriptor: {
            name: "test_stream",
            namespace: undefined,
          },
          destinationObjectName: "",
          sourceSyncMode: "full_refresh",
          destinationSyncMode: "append",
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
          matchingKeys: null,
          cursorField: null,
        },
      ],
    };
    expect(createFormDefaultValues(syncCatalogWithEncryptionMapper)).toEqual(expectedDefaultValues);
  });
});

describe(`${getMatchingKeysFromConfig.name}`, () => {
  it("should return primaryKey as matchingKeys if it is populated", () => {
    const config: AirbyteStreamConfiguration = {
      mappers: [
        {
          type: "field-renaming",
          mapperConfiguration: {
            originalFieldName: "source-pk",
            newFieldName: "destination-matching-key",
          },
        },
      ],
      primaryKey: [["source-pk"]],
      syncMode: "full_refresh",
      destinationSyncMode: "append",
    };

    expect(getMatchingKeysFromConfig(config)).toEqual(["destination-matching-key"]);
  });

  it("should return composite primaryKey as multiple matchingKeys if populated", () => {
    const config: AirbyteStreamConfiguration = {
      mappers: [
        {
          type: "field-renaming",
          mapperConfiguration: {
            originalFieldName: "source-pk",
            newFieldName: "destination-matching-key",
          },
        },
        {
          type: "field-renaming",
          mapperConfiguration: {
            originalFieldName: "source-pk2",
            newFieldName: "destination-matching-key2",
          },
        },
      ],
      primaryKey: [["source-pk"], ["source-pk2"]],
      syncMode: "full_refresh",
      destinationSyncMode: "append",
    };

    expect(getMatchingKeysFromConfig(config)).toEqual(["destination-matching-key", "destination-matching-key2"]);
  });

  it("should return null if not all primaryKey fields have corresponding mappers", () => {
    const config: AirbyteStreamConfiguration = {
      mappers: [
        {
          type: "field-renaming",
          mapperConfiguration: {
            originalFieldName: "source-pk",
            newFieldName: "destination-matching-key",
          },
        },
      ],
      primaryKey: [["source-pk"], ["source-pk2"]],
      syncMode: "full_refresh",
      destinationSyncMode: "append",
    };
    // In this case, the primary key has been selected, but the fields have not been fully mapped, so we fall back to
    // null, forcing the user to fix the issue in the UI.
    expect(getMatchingKeysFromConfig(config)).toBeNull();
  });

  it("should return null if primaryKey is not defined", () => {
    const config: AirbyteStreamConfiguration = {
      mappers: [],
      primaryKey: undefined,
      syncMode: "full_refresh",
      destinationSyncMode: "append",
    };

    expect(getMatchingKeysFromConfig(config)).toBeNull();
  });

  it("should return null if primaryKey is empty", () => {
    const config: AirbyteStreamConfiguration = {
      mappers: [],
      primaryKey: [],
      syncMode: "full_refresh",
      destinationSyncMode: "append",
    };

    expect(getMatchingKeysFromConfig(config)).toBeNull();
  });
});

const PARTIAL_HUBSPOT_SYNC_CATALOG: AirbyteCatalog = {
  streams: [
    {
      stream: {
        name: "campaigns",
        jsonSchema: {
          $schema: "http://json-schema.org/draft-07/schema#",
          additionalProperties: true,
          type: ["null", "object"],
          properties: {
            counters_open: {
              description: "Alias for the open counter value.",
              type: ["null", "integer"],
            },
            counters_dropped: {
              description: "Alias for the dropped counter value.",
              type: ["null", "integer"],
            },
            counters_click: {
              description: "Alias for the click counter value.",
              type: ["null", "integer"],
            },
            subject: {
              description: "The subject line of the campaign.",
              type: ["null", "string"],
            },
            contentId: {
              description: "The unique identifier of the content associated with the campaign.",
              type: ["null", "integer"],
            },
            counters_processed: {
              description: "Alias for the processed counter value.",
              type: ["null", "integer"],
            },
            type: {
              description: "Type classification of the campaign.",
              type: ["null", "string"],
            },
            counters_mta_dropped: {
              description: "Alias for the MTA dropped counter value.",
              type: ["null", "integer"],
            },
            numIncluded: {
              description: "Number of recipients included in the campaign.",
              type: ["null", "integer"],
            },
            appId: {
              description: "The unique identifier of the application associated with the campaign data.",
              type: ["null", "integer"],
            },
            counters_print: {
              description: "Alias for the print counter value.",
              type: ["null", "integer"],
            },
            lastUpdatedTime: {
              description: "Timestamp indicating when the campaign data was last updated.",
              type: ["null", "integer"],
            },
            id: {
              description: "The unique identifier of the campaign.",
              type: ["null", "integer"],
            },
            lastProcessingStateChangeAt: {
              description: "Timestamp indicating the last state change time of the processing state.",
              type: ["null", "integer"],
            },
            counters: {
              description: "Object containing different counters related to the campaign's performance.",
              type: ["null", "object"],
              properties: {
                deferred: {
                  description: "Number of deferred emails.",
                  type: ["null", "integer"],
                },
                bounce: {
                  description: "Number of bounced emails.",
                  type: ["null", "integer"],
                },
                forward: {
                  description: "Number of emails forwarded by recipients.",
                  type: ["null", "integer"],
                },
                dropped: {
                  description: "Number of dropped emails.",
                  type: ["null", "integer"],
                },
                delivered: {
                  description: "Number of successfully delivered emails.",
                  type: ["null", "integer"],
                },
                sent: {
                  description: "Number of emails sent.",
                  type: ["null", "integer"],
                },
                click: {
                  description: "Number of clicks on the campaign.",
                  type: ["null", "integer"],
                },
                spamreport: {
                  description: "Number of spam reports received for the campaign.",
                  type: ["null", "integer"],
                },
                processed: {
                  description: "Number of emails processed by the campaign.",
                  type: ["null", "integer"],
                },
                print: {
                  description: "Number of emails printed by recipients.",
                  type: ["null", "integer"],
                },
                unsubscribed: {
                  description: "Number of recipients unsubscribed from the campaign.",
                  type: ["null", "integer"],
                },
                statuschange: {
                  description: "Number of status changes related to the campaign.",
                  type: ["null", "integer"],
                },
                mta_dropped: {
                  description: "Number of emails dropped at the MTA level.",
                  type: ["null", "integer"],
                },
                suppressed: {
                  description: "Number of emails suppressed from sending.",
                  type: ["null", "integer"],
                },
                reply: {
                  description: "Number of replies received to the campaign.",
                  type: ["null", "integer"],
                },
                open: {
                  description: "Number of email opens.",
                  type: ["null", "integer"],
                },
              },
            },
            counters_bounce: {
              description: "Alias for the bounce counter value.",
              type: ["null", "integer"],
            },
            lastProcessingFinishedAt: {
              description: "Timestamp indicating when the last processing of the campaign was finished.",
              type: ["null", "integer"],
            },
            appName: {
              description: "The name of the application associated with the campaign data.",
              type: ["null", "string"],
            },
            counters_unsubscribed: {
              description: "Alias for the unsubscribed counter value.",
              type: ["null", "integer"],
            },
            counters_delivered: {
              description: "Alias for the delivered counter value.",
              type: ["null", "integer"],
            },
            counters_statuschange: {
              description: "Alias for the status change counter value.",
              type: ["null", "integer"],
            },
            numQueued: {
              description: "Number of emails queued for sending.",
              type: ["null", "integer"],
            },
            counters_forward: {
              description: "Alias for the forward counter value.",
              type: ["null", "integer"],
            },
            counters_suppressed: {
              description: "Alias for the suppressed counter value.",
              type: ["null", "integer"],
            },
            processingState: {
              description: "Current processing state of the campaign.",
              type: ["null", "string"],
            },
            counters_sent: {
              description: "Alias for the sent counter value.",
              type: ["null", "integer"],
            },
            counters_reply: {
              description: "Alias for the reply counter value.",
              type: ["null", "integer"],
            },
            counters_deferred: {
              description: "Alias for the deferred counter value.",
              type: ["null", "integer"],
            },
            name: {
              description: "The name of the campaign.",
              type: ["null", "string"],
            },
            subType: {
              description: "Subtype of the campaign.",
              type: ["null", "string"],
            },
            counters_spamreport: {
              description: "Alias for the spam report counter value.",
              type: ["null", "integer"],
            },
            lastProcessingStartedAt: {
              description: "Timestamp indicating when the last processing of the campaign started.",
              type: ["null", "integer"],
            },
          },
        },
        supportedSyncModes: ["full_refresh", "incremental"],
        sourceDefinedCursor: true,
        defaultCursorField: ["lastUpdatedTime"],
        sourceDefinedPrimaryKey: [["id"]],
        isResumable: true,
        isFileBased: false,
      },
      config: {
        syncMode: "full_refresh",
        cursorField: ["lastUpdatedTime"],
        destinationSyncMode: "append",
        primaryKey: [["id"]],
        aliasName: "campaigns",
        selected: true,
        suggested: false,
        selectedFields: [],
        hashedFields: [],
        mappers: [
          {
            id: "mapper1",
            type: StreamMapperType["field-renaming"],
            mapperConfiguration: {
              originalFieldName: "id",
              newFieldName: "destination_id",
            },
          },
        ],
      },
    },
    {
      stream: {
        name: "companies_property_history",
        jsonSchema: {
          path: "/crm/v3/objects/companies",
          $schema: "http://json-schema.org/draft-07/schema",
          name: "companies_property_history",
          extractor_field_path: "results",
          additionalProperties: true,
          type: ["null", "object"],
          properties: {
            sourceId: {
              description: "The identifier of the source that updated the property in the company record.",
              type: ["null", "string"],
            },
            archived: {
              description: "Flag indicating if the company property history record is archived or not.",
              type: ["null", "boolean"],
            },
            companyId: {
              description: "The unique identifier of the company to which the property history record belongs.",
              type: ["null", "string"],
            },
            sourceType: {
              description: "The type of the source that updated the property in the company record.",
              type: ["null", "string"],
            },
            property: {
              description: "The specific property that was updated in the company record.",
              type: ["null", "string"],
            },
            updatedByUserId: {
              description: "The user ID of the user who initiated the property update.",
              type: ["null", "number"],
            },
            value: {
              description: "The new value of the property after the update.",
              type: ["null", "string"],
            },
            timestamp: {
              format: "date-time",
              airbyte_type: "timestamp_with_timezone",
              description: "The date and time when the property update occurred.",
              type: ["null", "string"],
            },
          },
          $parameters: {
            path: "/crm/v3/objects/companies",
            name: "companies_property_history",
            extractor_field_path: "results",
          },
        },
        supportedSyncModes: ["full_refresh", "incremental"],
        sourceDefinedCursor: true,
        defaultCursorField: ["timestamp"],
        sourceDefinedPrimaryKey: [["companyId"], ["property"], ["timestamp"]],
        isResumable: true,
        isFileBased: false,
      },
      config: {
        syncMode: "full_refresh",
        cursorField: ["timestamp"],
        destinationSyncMode: "append",
        primaryKey: [["companyId"], ["property"], ["timestamp"]],
        aliasName: "companies_property_history",
        selected: false,
        suggested: false,
        selectedFields: [],
        hashedFields: [],
        mappers: [],
      },
    },
  ],
};
