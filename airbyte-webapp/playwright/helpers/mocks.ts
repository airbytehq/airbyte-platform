import { Page } from "@playwright/test";
import { AirbyteCatalog, SyncMode } from "@src/core/api/types/AirbyteClient";

// This file contains mocking helpers to handle endpoint calls we don't want
// to make in our tests. This allows us to avoid adding complex handling of random server failures on
// non-essential calls, or calls to external APIs that we don't control.

// ============================================================================
// Mock Catalog Creators
// ============================================================================
// These catalog creators are used in two different contexts across the test suite:
//
// 1. API-level mocking (APIRequestContext):
//    - Used by connectionAPI.create(..., { useMockSchemaDiscovery: true })
//    - Returns catalog directly, no HTTP call made
//    - Use case: Test setup/scaffolding  (example: configuration.spec.ts)
//
// 2. Page-level mocking (Browser):
//    - Used by mockHelpers.mockSchemaDiscovery(page, sourceType)
//    - Intercepts browser HTTP requests via page.route()
//    - Use case: Browser-based flows in main test logic (example: createConnection.spec.ts)
//
// Both methods use the same catalog data to ensure consistency.
// ============================================================================

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

/**
 * Wraps a catalog in the full API response format for schema discovery.
 *
 * The actual /sources/discover_schema endpoint returns an object with both
 * the catalog and jobInfo metadata. This helper creates that wrapper for
 * use in page.route() mocking to simulate the complete API response.
 */
const createDiscoveryResponse = (catalog: AirbyteCatalog) => ({
  catalog,
  jobInfo: {
    id: "mock-discover-job-id",
    configType: "discover_schema",
    createdAt: Date.now(),
    endedAt: Date.now() + 1000,
    succeeded: true,
    connectorConfigurationUpdated: false,
    logs: { logLines: [] },
  },
});

export const mockHelpers = {
  // Mock enterprise connector stubs (used by both source and destination)
  mockEnterpriseConnectors: async (page: Page) => {
    await page.route("**/api/v1/source_definitions/list_enterprise_stubs_for_workspace", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ enterpriseConnectorStubs: [] }),
      });
    });

    await page.route("**/api/v1/destination_definitions/list_enterprise_stubs_for_workspace", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ enterpriseConnectorStubs: [] }),
      });
    });
  },

  // Mock successful connection check
  mockConnectionCheck: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint =
      connectorType === "source"
        ? "**/api/v1/scheduler/sources/check_connection"
        : "**/api/v1/scheduler/destinations/check_connection";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          status: "succeeded",
          message: "Connection test succeeded",
          jobInfo: {
            id: "test-job-id",
            configType: `check_connection_${connectorType}`,
            createdAt: Date.now(),
            endedAt: Date.now() + 1000,
            succeeded: true,
            connectorConfigurationUpdated: false,
            logs: { logLines: [] },
          },
        }),
      });
    });
  },

  // Mock failed connection check
  mockFailedConnectionCheck: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint =
      connectorType === "source"
        ? "**/api/v1/scheduler/sources/check_connection"
        : "**/api/v1/scheduler/destinations/check_connection";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          status: "failed",
          message: "Something went wrong",
          jobInfo: {
            id: "test-job-id",
            configType: `check_connection_${connectorType}`,
            createdAt: Date.now(),
            endedAt: Date.now() + 1000,
            succeeded: false,
            connectorConfigurationUpdated: false,
            logs: { logLines: [] },
          },
        }),
      });
    });
  },

  // Mock empty connector lists for redirect tests
  mockEmptyConnectorLists: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint = connectorType === "source" ? "**/api/v1/sources/list" : "**/api/v1/destinations/list";
    const responseKey = connectorType === "source" ? "sources" : "destinations";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ [responseKey]: [] }),
      });
    });
  },

  /**
   * Mock schema discovery responses for browser-based flows.
   * Intercepts HTTP requests made by the browser via page.route().
   *
   * @param page - Playwright page object
   * @param sourceType - Type of source connector to mock ('postgres' or 'pokeapi')
   *
   * Usage in tests:
   * ```
   * test.beforeAll(async ({ browser }) => {
   *   context = await browser.newContext();
   *   page = await context.newPage();
   *   await mockHelpers.mockSchemaDiscovery(page, 'postgres');
   * });
   * ```
   */
  mockSchemaDiscovery: async (page: Page, sourceType: "postgres" | "pokeapi" = "postgres") => {
    const catalog = sourceType === "postgres" ? createMockPostgresCatalog() : createMockPokeApiCatalog();
    const mockResponse = createDiscoveryResponse(catalog);

    await page.route("**/api/v1/sources/discover_schema", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(mockResponse),
      });
    });
  },
};
