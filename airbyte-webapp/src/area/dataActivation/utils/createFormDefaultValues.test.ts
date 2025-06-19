import { AirbyteCatalog } from "core/api/types/AirbyteClient";

import { createFormDefaultValues } from "./createFormDefaultValues";

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
            primaryKey: [],
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
          primaryKey: null,
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
          fields: [],
          primaryKey: "id",
          cursorField: "lastUpdatedTime",
        },
      ],
    };

    expect(createFormDefaultValues(PARTIAL_HUBSPOT_SYNC_CATALOG)).toEqual(expectedDefaultValues);
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
        mappers: [],
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
