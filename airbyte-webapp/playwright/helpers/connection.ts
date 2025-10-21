import { APIRequestContext, Page, expect } from "@playwright/test";
import {
  DestinationRead,
  SourceRead,
  WebBackendConnectionRead,
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  SyncMode,
  DestinationSyncMode,
  SourceDiscoverSchemaRead,
} from "@src/core/api/types/AirbyteClient";

import { getApiBaseUrl } from "./api";
import {
  pokeSourceAPI,
  fakerSourceAPI,
  e2eDestinationAPI,
  postgresSourceAPI,
  postgresDestinationAPI,
} from "./connectors";
import { createMockPokeApiCatalog, createMockPostgresCatalog } from "./mocks";
import { appendRandomString } from "./ui";

/**
 * Connection Test Helpers
 *
 * This file contains a set of helpers for wiring up connections in our Playwright tests:
 *
 * - connectionAPI: CRUD operations for connections (create, delete, get, update)
 * - connectionUI: UI navigation and interaction helpers (visit pages, start/cancel syncs, verify status)
 * - jobUI: Job event and modal helpers for timeline/status tests
 * - connectionTestHelpers: Test setup helpers for creating connector pairs and cleanup
 * - connectionForm: Form interaction helpers for connection settings page
 * - connectionTestScaffold: Reusable test scaffolding patterns for connection test suites
 *
 * Currently supported connector pairs: "poke-e2e", "faker-e2e", "postgres-postgres", "poke-postgres"
 */

// Type for connector API helpers - all have same structure
type ConnectorAPI = typeof pokeSourceAPI;
type ConnectorPair = "poke-e2e" | "faker-e2e" | "postgres-postgres" | "poke-postgres";

// Create connection API interface following the established connector pattern
export const connectionAPI = {
  // Create a new connection between source and destination
  create: async (
    request: APIRequestContext,
    source: SourceRead,
    destination: DestinationRead,
    options: {
      enableAllStreams?: boolean;
      useMockSchemaDiscovery?: boolean;
      mockSourceType?: "postgres" | "pokeapi";
    } = {}
  ): Promise<WebBackendConnectionRead> => {
    const apiBaseUrl = getApiBaseUrl();

    let typedCatalog: AirbyteCatalog;

    // Mock schema discovery to avoid overloading test databases in CI environments.
    //
    // In CI, concurrent test runs making real schema discovery calls can overwhelm
    // external resources, causing timeouts and flaky test failures.
    // See mocks.ts for more context

    if (options.useMockSchemaDiscovery) {
      const mockSourceType = options.mockSourceType || "postgres";
      typedCatalog = mockSourceType === "postgres" ? createMockPostgresCatalog() : createMockPokeApiCatalog();
    } else {
      // First, discover the source schema
      const discoverResponse = await request.post(`${apiBaseUrl}/sources/discover_schema`, {
        data: {
          sourceId: source.sourceId,
          disable_cache: true,
        },
      });

      if (!discoverResponse.ok()) {
        const errorText = await discoverResponse.text().catch(() => "Unknown error");
        throw new Error(`Failed to discover schema: ${discoverResponse.status()} - ${errorText}`);
      }

      const discoverResult = (await discoverResponse.json()) as SourceDiscoverSchemaRead;
      const catalog = discoverResult.catalog;

      if (!catalog || !catalog.streams) {
        console.error("Schema discovery failed - invalid catalog:", discoverResult);
        throw new Error(`Schema discovery returned invalid catalog: ${JSON.stringify(discoverResult)}`);
      }

      typedCatalog = catalog;
    }

    // Prepare streams based on options
    let streams: AirbyteStreamAndConfiguration[];
    if (options.enableAllStreams) {
      streams = typedCatalog.streams.map((stream) => ({
        ...stream,
        config: {
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
          selected: true,
        },
      }));
    } else {
      // Default to no streams selected
      streams = typedCatalog.streams.map((stream) => ({
        ...stream,
        config: {
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
          selected: false,
        },
      }));
    }

    // Create the connection
    const connectionData = {
      name: `${source.name} → ${destination.name}`,
      sourceId: source.sourceId,
      destinationId: destination.destinationId,
      syncCatalog: { streams },
      scheduleType: "manual",
      status: "active",
    };

    const response = await request.post(`${apiBaseUrl}/web_backend/connections/create`, {
      data: connectionData,
    });

    if (!response.ok()) {
      throw new Error(`Failed to create connection: ${response.status()}`);
    }

    const result = await response.json();
    return result as WebBackendConnectionRead;
  },

  // Delete a connection
  delete: async (request: APIRequestContext, connectionId: string): Promise<void> => {
    const apiBaseUrl = getApiBaseUrl();

    const response = await request.post(`${apiBaseUrl}/connections/delete`, {
      data: { connectionId },
    });

    if (!response.ok()) {
      throw new Error(`Failed to delete connection: ${response.status()}`);
    }
  },

  // Get a connection (with optional schema refresh)
  get: async (
    request: APIRequestContext,
    connectionId: string,
    options: { withRefreshedCatalog?: boolean } = {}
  ): Promise<WebBackendConnectionRead> => {
    const apiBaseUrl = getApiBaseUrl();

    const response = await request.post(`${apiBaseUrl}/web_backend/connections/get`, {
      data: {
        connectionId,
        withRefreshedCatalog: options.withRefreshedCatalog ?? false,
      },
    });

    if (!response.ok()) {
      throw new Error(`Failed to get connection: ${response.status()}`);
    }

    return await response.json();
  },

  // Update a connection
  update: async (
    request: APIRequestContext,
    connectionId: string,
    updates: Partial<WebBackendConnectionRead>
  ): Promise<WebBackendConnectionRead> => {
    const apiBaseUrl = getApiBaseUrl();

    // Merge updates with current connection
    const updateData = {
      ...updates,
      connectionId,
    };

    const response = await request.post(`${apiBaseUrl}/web_backend/connections/update`, {
      data: updateData,
    });

    if (!response.ok()) {
      throw new Error(`Failed to update connection: ${response.status()}`);
    }

    return await response.json();
  },
};

// UI navigation helpers for connection pages
export const connectionUI = {
  // Visit a specific connection page tab
  visit: async (page: Page, connection: WebBackendConnectionRead, tab: string = ""): Promise<void> => {
    // Get workspaceId from the source object
    const workspaceId = connection.source.workspaceId;
    const connectionId = connection.connectionId;

    let url = `/workspaces/${workspaceId}/connections/${connectionId}`;
    if (tab) {
      url += `/${tab}`;
    }

    await page.goto(url, { timeout: 10000 });
  },

  // Start a manual sync
  startManualSync: async (page: Page): Promise<void> => {
    return page.locator("[data-testid='manual-sync-button']").click();
  },

  // Start a manual reset (for tests that need it)
  startManualReset: async (page: Page): Promise<void> => {
    // Open the dropdown menu
    await page.locator("[data-testid='job-history-dropdown-menu']").click();

    // Click reset option
    await page.locator("[data-testid='reset-data-dropdown-option']").click();

    // Confirm the reset
    return page.locator("[data-id='clear-data']").click();
  },

  // Cancel an ongoing sync
  cancelSync: async (page: Page): Promise<void> => {
    await page.locator("[data-testid='cancel-sync-button']").click();
    return page.locator("text=Yes, cancel sync").click();
  },

  // Verify sync status messages
  verifySyncStatus: {
    running: async (page: Page): Promise<void> => {
      await expect(page.locator("text=manually started a sync")).toBeVisible({ timeout: 10000 });
      return expect(page.locator("text=Sync running")).toBeVisible({ timeout: 10000 });
    },
    cancelled: async (page: Page): Promise<void> => {
      return expect(page.locator("text=Sync cancelled")).toBeVisible({ timeout: 10000 });
    },
  },
};

// Job event and modal helpers for timeline/status tests
export const jobUI = {
  // Open job event menu
  openEventMenu: async (page: Page): Promise<void> => {
    const eventMenuButton = page.locator("[data-testid='job-event-menu']").locator("button");
    await eventMenuButton.waitFor({ state: "visible", timeout: 10000 });
    return eventMenuButton.click();
  },

  // Copy link to event from menu
  copyEventLink: async (page: Page): Promise<string> => {
    await page.locator("[data-testid='copy-link-to-event']").click({ force: true });
    return await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
  },

  // Open job logs modal from menu
  openLogsFromMenu: async (page: Page): Promise<void> => {
    await page.locator("[data-testid='view-logs']").click();
    return expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 15000 });
  },

  // Copy attempt link from modal
  copyAttemptLink: async (page: Page): Promise<string> => {
    await page.locator("[data-testid='copy-link-to-attempt-button']").click();
    return await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
  },

  // Close job logs modal
  closeLogsModal: async (page: Page): Promise<void> => {
    return page.locator("[data-testid='close-modal-button']").click();
  },

  // Verify job logs modal is open
  verifyLogsModal: async (page: Page): Promise<void> => {
    return expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 30000 });
  },
};

// Test setup helpers for connection tests
export const connectionTestHelpers = {
  // Unified connector setup with programmatic pairing
  setupConnectors: async (
    request: APIRequestContext,
    workspaceId: string,
    connectorType: ConnectorPair = "poke-e2e",
    sourcePrefix?: string,
    destinationPrefix?: string
  ) => {
    // Determine connector APIs and default names based on type
    const configs: Record<
      ConnectorPair,
      {
        sourceAPI: ConnectorAPI;
        destinationAPI: ConnectorAPI;
        defaultSourcePrefix: string;
        defaultDestinationPrefix: string;
      }
    > = {
      "poke-e2e": {
        sourceAPI: pokeSourceAPI,
        destinationAPI: e2eDestinationAPI,
        defaultSourcePrefix: "PokeAPI source",
        defaultDestinationPrefix: "E2E Testing destination",
      },
      "faker-e2e": {
        sourceAPI: fakerSourceAPI,
        destinationAPI: e2eDestinationAPI,
        defaultSourcePrefix: "Faker source",
        defaultDestinationPrefix: "E2E Testing destination",
      },
      "postgres-postgres": {
        sourceAPI: postgresSourceAPI,
        destinationAPI: postgresDestinationAPI,
        defaultSourcePrefix: "Postgres source",
        defaultDestinationPrefix: "Postgres destination",
      },
      "poke-postgres": {
        sourceAPI: pokeSourceAPI,
        destinationAPI: postgresDestinationAPI,
        defaultSourcePrefix: "PokeAPI source",
        defaultDestinationPrefix: "Postgres destination",
      },
    };

    const config = configs[connectorType];
    const sourceName = appendRandomString(sourcePrefix ?? config.defaultSourcePrefix);
    const destinationName = appendRandomString(destinationPrefix ?? config.defaultDestinationPrefix);

    const source = (await config.sourceAPI.create(request, sourceName, workspaceId)) as SourceRead;
    const destination = (await config.destinationAPI.create(request, destinationName, workspaceId)) as DestinationRead;

    return {
      source,
      destination,
      sourceId: source.sourceId,
      destinationId: destination.destinationId,
    };
  },

  // Backward compatibility aliases (delegate to unified function)
  setupConnectionTest: async (
    request: APIRequestContext,
    workspaceId: string,
    sourcePrefix?: string,
    destinationPrefix?: string
  ) => connectionTestHelpers.setupConnectors(request, workspaceId, "poke-e2e", sourcePrefix, destinationPrefix),

  setupPostgresConnectionTest: async (
    request: APIRequestContext,
    workspaceId: string,
    sourcePrefix?: string,
    destinationPrefix?: string
  ) =>
    connectionTestHelpers.setupConnectors(request, workspaceId, "postgres-postgres", sourcePrefix, destinationPrefix),

  setupFakerConnectionTest: async (
    request: APIRequestContext,
    workspaceId: string,
    sourcePrefix?: string,
    destinationPrefix?: string
  ) => connectionTestHelpers.setupConnectors(request, workspaceId, "faker-e2e", sourcePrefix, destinationPrefix),

  setupPokeApiPostgresConnectionTest: async (
    request: APIRequestContext,
    workspaceId: string,
    sourcePrefix?: string,
    destinationPrefix?: string
  ) => connectionTestHelpers.setupConnectors(request, workspaceId, "poke-postgres", sourcePrefix, destinationPrefix),

  // Cleanup connection test resources with smart connector deletion
  cleanupConnectionTest: async (
    request: APIRequestContext,
    resources: {
      connectionId?: string;
      sourceId?: string;
      destinationId?: string;
    }
  ) => {
    const { connectionId, sourceId, destinationId } = resources;

    // Helper to attempt deletion and log warnings
    const tryDelete = async (deleteFunc: () => Promise<void>, resourceType: string, resourceId: string) => {
      try {
        await deleteFunc();
      } catch (error) {
        console.warn(`⚠️ Failed to clean up ${resourceType} ${resourceId}:`, error);
      }
    };

    // Delete connection first (depends on source/destination)
    if (connectionId) {
      await tryDelete(() => connectionAPI.delete(request, connectionId), "connection", connectionId);
    }

    // Delete source and destination in parallel
    const cleanupPromises: Array<Promise<void>> = [];

    if (sourceId) {
      // Delete whichever source API was used in a given test
      cleanupPromises.push(
        tryDelete(() => pokeSourceAPI.delete(request, sourceId), "source", sourceId),
        tryDelete(() => postgresSourceAPI.delete(request, sourceId), "source", sourceId),
        tryDelete(() => fakerSourceAPI.delete(request, sourceId), "source", sourceId)
      );
    }

    if (destinationId) {
      // Try both destination APIs since we don't know which was used
      cleanupPromises.push(
        tryDelete(() => e2eDestinationAPI.delete(request, destinationId), "destination", destinationId),
        tryDelete(() => postgresDestinationAPI.delete(request, destinationId), "destination", destinationId)
      );
    }

    // Wait for all cleanup operations to complete
    await Promise.allSettled(cleanupPromises);
  },
};

// Connection form helpers for settings page interactions
export const connectionForm = {
  // Toggle the advanced settings section
  toggleAdvancedSettings: async (page: Page): Promise<void> => {
    const button = page.locator('[data-testid="advanced-settings-button"]');
    await button.click();
    // Wait for advanced settings content to expand
    // Use a simple timeout since fields may vary based on connection state
    await page.waitForTimeout(1000);
  },

  // Select schedule type: "Scheduled", "Manual", or "Cron"
  selectScheduleType: async (page: Page, scheduleType: "Scheduled" | "Manual" | "Cron"): Promise<void> => {
    const button = page.locator('[data-testid="schedule-type-listbox-button"]');

    // Click to open the dropdown
    await button.click();

    // Wait for dropdown to be visible
    await page.waitForTimeout(300);

    // Click directly on the desired option
    const option = page.locator(`[data-testid="${scheduleType.toLowerCase()}-option"]`);
    await option.waitFor({ state: "visible", timeout: 5000 });
    await option.click();

    // Wait for dropdown to close and verify selection
    await expect(button).toContainText(scheduleType, { timeout: 5000 });
  },

  // Select basic schedule interval (e.g., "1-hours", "24-hours")
  selectBasicScheduleData: async (page: Page, interval: string): Promise<void> => {
    const button = page.locator('[data-testid="basic-schedule-listbox-button"]');

    // Click to open the dropdown
    await button.click();

    // Find the option using data-testid and click it (this dropdown may have many options)
    const option = page.locator(`[data-testid="frequency-${interval.toLowerCase()}-option"]`);
    await option.waitFor({ state: "visible", timeout: 5000 });
    await option.click();
  },

  // Setup destination namespace with custom format
  setupDestinationNamespaceCustomFormat: async (page: Page, customValue: string): Promise<void> => {
    const namespaceButton = page.locator('[data-testid="namespace-definition-listbox-button"]');
    await namespaceButton.waitFor({ state: "visible", timeout: 10000 });
    await namespaceButton.click();
    await page.locator('[data-testid="custom-option"]').click();
    const input = page.locator('[data-testid="namespace-definition-custom-format-input"]').first();
    // Fill with the full format including ${SOURCE_NAMESPACE} prefix
    await input.fill(`\${SOURCE_NAMESPACE}${customValue}`);
  },

  // Setup destination namespace with source-defined option
  setupDestinationNamespaceSourceFormat: async (page: Page): Promise<void> => {
    await page.locator('[data-testid="namespace-definition-listbox-button"]').click();
    return page.locator('[data-testid="source-option"]').click({ force: true });
  },

  // Setup destination namespace with destination default option
  setupDestinationNamespaceDestinationFormat: async (page: Page): Promise<void> => {
    const namespaceButton = page.locator('[data-testid="namespace-definition-listbox-button"]');
    await namespaceButton.waitFor({ state: "visible", timeout: 10000 });
    await namespaceButton.click();
    return page.locator('[data-testid="destination-option"]').click({ force: true });
  },

  // Set stream prefix
  setStreamPrefix: async (page: Page, prefix: string): Promise<void> => {
    const input = page.locator('[data-testid="stream-prefix-input"]');
    return input.fill(prefix);
  },

  // Clear stream prefix
  clearStreamPrefix: async (page: Page): Promise<void> => {
    const input = page.locator('[data-testid="stream-prefix-input"]');
    return input.clear();
  },

  // Submit the form
  submit: async (page: Page): Promise<void> => {
    return page.locator("button[type='submit']").click();
  },

  // Wait for and verify success notification
  verifySuccessNotification: async (page: Page): Promise<void> => {
    return expect(page.locator("text=Your changes were saved!")).toBeVisible({ timeout: 10000 });
  },

  // Complete namespace update flow: open advanced, set custom format, verify preview
  updateDestinationNamespaceCustom: async (page: Page, customValue: string, expectedPreview: string): Promise<void> => {
    await connectionForm.toggleAdvancedSettings(page);
    await connectionForm.setupDestinationNamespaceCustomFormat(page, customValue);
    const preview = page.locator('[data-testid="custom-namespace-preview"]');
    return expect(preview).toHaveText(expectedPreview, { timeout: 5000 });
  },

  // Complete prefix update flow: set prefix, verify preview
  updateStreamPrefix: async (page: Page, prefix: string): Promise<void> => {
    await connectionForm.setStreamPrefix(page, prefix);
    const prefixPreview = page.locator('[data-testid="stream-prefix-preview"]');
    return expect(prefixPreview).toContainText(prefix, { timeout: 5000 });
  },

  // Verify a preview element contains expected text
  verifyPreview: async (page: Page, testId: string, expectedText: string): Promise<void> => {
    const preview = page.locator(`[data-testid="${testId}"]`);
    return expect(preview).toContainText(expectedText, { timeout: 5000 });
  },

  // Verify a preview element is not visible
  verifyPreviewNotVisible: async (page: Page, testId: string): Promise<void> => {
    const preview = page.locator(`[data-testid="${testId}"]`);
    return expect(preview).not.toBeVisible();
  },
};

// Reusable test scaffolding for connection test suites
// This provides a pattern for managing connection lifecycle in tests
export const connectionTestScaffold = {
  /**
   * Creates a connection test scaffold with automatic setup/cleanup
   * Use in beforeAll/afterAll hooks to eliminate boilerplate
   *
   * @example
   * test.beforeAll(async ({ request }) => {
   *   testData = await connectionTestScaffold.setupConnection(
   *     request, workspaceId, "postgres-postgres", { useMockSchemaDiscovery: true }
   *   );
   * });
   */
  setupConnection: async (
    request: APIRequestContext,
    workspaceId: string,
    connectorType: ConnectorPair = "poke-e2e",
    options: {
      enableAllStreams?: boolean;
      status?: "active" | "inactive";
      useMockSchemaDiscovery?: boolean;
    } = {}
  ) => {
    const testResources = await connectionTestHelpers.setupConnectors(request, workspaceId, connectorType);

    // Determine mock source type based on connector type
    const mockSourceType = connectorType === "poke-e2e" || connectorType === "poke-postgres" ? "pokeapi" : "postgres";

    const connection = await connectionAPI.create(request, testResources.source, testResources.destination, {
      enableAllStreams: options.enableAllStreams ?? true,
      useMockSchemaDiscovery: options.useMockSchemaDiscovery,
      mockSourceType,
    });

    // Apply status if specified
    if (options.status === "inactive") {
      await connectionAPI.update(request, connection.connectionId, { status: "inactive" });
    }

    return {
      connection,
      sourceId: testResources.sourceId,
      destinationId: testResources.destinationId,
      source: testResources.source,
      destination: testResources.destination,
    };
  },

  /**
   * Cleans up connection test resources
   * Use in afterAll hooks
   */
  cleanupConnection: (
    request: APIRequestContext,
    testData: {
      connection?: WebBackendConnectionRead;
      sourceId?: string;
      destinationId?: string;
    }
  ) => {
    return connectionTestHelpers.cleanupConnectionTest(request, {
      connectionId: testData.connection?.connectionId,
      sourceId: testData.sourceId,
      destinationId: testData.destinationId,
    });
  },
};
