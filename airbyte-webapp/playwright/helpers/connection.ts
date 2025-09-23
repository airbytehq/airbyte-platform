import { APIRequestContext, Page, expect } from "@playwright/test";
import {
  DestinationRead,
  SourceRead,
  WebBackendConnectionRead,
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  SyncMode,
  DestinationSyncMode,
} from "@src/core/api/types/AirbyteClient";

import { getApiBaseUrl } from "./api";
import { sourceAPI, destinationAPI } from "./connectors";
import { appendRandomString } from "./ui";

// This file contains helper methods for connection CRUD operations

// Create connection API interface following the established connector pattern
export const connectionAPI = {
  // Create a new connection between source and destination
  create: async (
    request: APIRequestContext,
    source: SourceRead,
    destination: DestinationRead,
    options: { enableAllStreams?: boolean } = {}
  ): Promise<WebBackendConnectionRead> => {
    const apiBaseUrl = getApiBaseUrl();

    // First, discover the source schema
    const discoverResponse = await request.post(`${apiBaseUrl}/sources/discover_schema`, {
      data: {
        sourceId: source.sourceId,
        disable_cache: true,
      },
    });

    if (!discoverResponse.ok()) {
      throw new Error(`Failed to discover schema: ${discoverResponse.status()}`);
    }

    const { catalog }: { catalog: AirbyteCatalog } = await discoverResponse.json();

    // Prepare streams based on options
    let streams: AirbyteStreamAndConfiguration[];
    if (options.enableAllStreams) {
      streams = catalog.streams.map((stream) => ({
        ...stream,
        config: {
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
          selected: true,
        },
      }));
    } else {
      // Default to no streams selected
      streams = catalog.streams.map((stream) => ({
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
    await page.locator("[data-testid='manual-sync-button']").click();
  },

  // Start a manual reset (for tests that need it)
  startManualReset: async (page: Page): Promise<void> => {
    // Open the dropdown menu
    await page.locator("[data-testid='job-history-dropdown-menu']").click();

    // Click reset option
    await page.locator("[data-testid='reset-data-dropdown-option']").click();

    // Confirm the reset
    await page.locator("[data-id='clear-data']").click();
  },

  // Cancel an ongoing sync
  cancelSync: async (page: Page): Promise<void> => {
    await page.locator("[data-testid='cancel-sync-button']").click();
    await page.locator("text=Yes, cancel sync").click();
  },

  // Verify sync status messages
  verifySyncStatus: {
    running: async (page: Page): Promise<void> => {
      await expect(page.locator("text=manually started a sync")).toBeVisible({ timeout: 10000 });
      await expect(page.locator("text=Sync running")).toBeVisible({ timeout: 10000 });
    },
    cancelled: async (page: Page): Promise<void> => {
      await expect(page.locator("text=Sync cancelled")).toBeVisible({ timeout: 10000 });
    },
  },
};

// Job event and modal helpers for timeline/status tests
export const jobUI = {
  // Open job event menu
  openEventMenu: async (page: Page): Promise<void> => {
    const eventMenuButton = page.locator("[data-testid='job-event-menu']").locator("button");
    await eventMenuButton.waitFor({ state: "visible", timeout: 10000 });
    await eventMenuButton.click();
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
    await expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 15000 });
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
    await page.locator("[data-testid='close-modal-button']").click();
  },

  // Verify job logs modal is open
  verifyLogsModal: async (page: Page): Promise<void> => {
    await expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 30000 });
  },
};

// Test setup helpers for connection tests
export const connectionTestHelpers = {
  // Setup connection test with source, destination, and connection
  setupConnectionTest: async (
    request: APIRequestContext,
    workspaceId: string,
    sourcePrefix: string = "Test source",
    destinationPrefix: string = "Test destination"
  ) => {
    const sourceName = appendRandomString(sourcePrefix);
    const destinationName = appendRandomString(destinationPrefix);

    const source = await sourceAPI.create(request, sourceName, workspaceId);
    const destination = await destinationAPI.create(request, destinationName, workspaceId);

    return {
      source,
      destination,
      sourceId: source.sourceId,
      destinationId: destination.destinationId,
    };
  },

  // Cleanup connection test resources
  cleanupConnectionTest: async (
    request: APIRequestContext,
    resources: {
      connectionId?: string;
      sourceId?: string;
      destinationId?: string;
    }
  ) => {
    const { connectionId, sourceId, destinationId } = resources;

    if (connectionId) {
      try {
        await connectionAPI.delete(request, connectionId);
      } catch (error) {
        console.warn(`⚠️ Failed to clean up connection ${connectionId}:`, error);
      }
    }

    if (sourceId) {
      try {
        await sourceAPI.delete(request, sourceId);
      } catch (error) {
        console.warn(`⚠️ Failed to clean up source ${sourceId}:`, error);
      }
    }

    if (destinationId) {
      try {
        await destinationAPI.delete(request, destinationId);
      } catch (error) {
        console.warn(`⚠️ Failed to clean up destination ${destinationId}:`, error);
      }
    }
  },
};
