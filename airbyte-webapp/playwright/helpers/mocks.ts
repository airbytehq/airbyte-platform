import { AirbyteCatalog, SyncMode } from "@src/core/api/types/AirbyteClient";

// Catalogs as source discovery returns them. The mock backend uses them in catalogs.postgres() and catalogs.pokeApi()
// (mock/builders/catalogs.ts). The older mocked tests use them directly (example: createConnection.spec.ts).

/** Creates mock catalog for Postgres test database (users, cars, cities tables) */
export const createMockPostgresCatalog = (): AirbyteCatalog => ({
  streams: [
    {
      stream: {
        name: "cars",
        namespace: "public",
        jsonSchema: {
          type: "object",
          properties: {
            color: { type: "string" },
            model: { type: "string" },
            id: { type: "number", airbyte_type: "integer" },
            mark: { type: "string" },
          },
        },
        supportedSyncModes: [SyncMode.full_refresh, SyncMode.incremental],
        sourceDefinedCursor: false,
        defaultCursorField: [],
        sourceDefinedPrimaryKey: [["id"]],
        isResumable: true,
      },
      config: {
        syncMode: SyncMode.full_refresh,
        cursorField: [],
        destinationSyncMode: "overwrite",
        primaryKey: [["id"]],
        aliasName: "cars",
        selected: false,
        suggested: true,
        selectedFields: [],
        hashedFields: [],
        mappers: [],
      },
    },
    {
      stream: {
        name: "cities",
        namespace: "public",
        jsonSchema: {
          type: "object",
          properties: {
            country: { type: "string" },
            city: { type: "string" },
            city_code: { type: "string" },
            id: { type: "number", airbyte_type: "integer" },
            state: { type: "string" },
          },
        },
        supportedSyncModes: [SyncMode.full_refresh, SyncMode.incremental],
        sourceDefinedCursor: false,
        defaultCursorField: [],
        sourceDefinedPrimaryKey: [],
        isResumable: true,
      },
      config: {
        syncMode: SyncMode.full_refresh,
        cursorField: [],
        destinationSyncMode: "overwrite",
        primaryKey: [],
        aliasName: "cities",
        selected: false,
        suggested: true,
        selectedFields: [],
        hashedFields: [],
        mappers: [],
      },
    },
    {
      stream: {
        name: "users",
        namespace: "public",
        jsonSchema: {
          type: "object",
          properties: {
            updated_at: { type: "string", format: "date-time", airbyte_type: "timestamp_without_timezone" },
            name: { type: "string" },
            id: { type: "number", airbyte_type: "integer" },
            email: { type: "string" },
          },
        },
        supportedSyncModes: [SyncMode.full_refresh, SyncMode.incremental],
        sourceDefinedCursor: false,
        defaultCursorField: [],
        sourceDefinedPrimaryKey: [["id"]],
        isResumable: true,
      },
      config: {
        syncMode: SyncMode.full_refresh,
        cursorField: [],
        destinationSyncMode: "overwrite",
        primaryKey: [["id"]],
        aliasName: "users",
        selected: false,
        suggested: true,
        selectedFields: [],
        hashedFields: [],
        mappers: [],
      },
    },
  ],
});

/** Creates mock catalog for PokeAPI source (pokemon stream, no namespace) */
export const createMockPokeApiCatalog = (): AirbyteCatalog => ({
  streams: [
    {
      stream: {
        name: "pokemon",
        jsonSchema: {
          type: "object",
          properties: {
            id: { type: "integer" },
            name: { type: "string" },
            base_experience: { type: "integer" },
            height: { type: "integer" },
            weight: { type: "integer" },
          },
        },
        supportedSyncModes: [SyncMode.full_refresh],
        sourceDefinedCursor: false,
        sourceDefinedPrimaryKey: [["id"]],
      },
    },
  ],
});
