import { test, expect } from "@playwright/test";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { connectionAPI, connectionUI, connectionTestHelpers } from "../../helpers/connection";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection Status - Faker + E2E", () => {
  let workspaceId: string;
  let source: SourceRead;
  let destination: DestinationRead;
  let connectionId: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.beforeEach(async ({ request }) => {
    // Create Faker source and E2E destination for each test
    // Faker generates fake data in memory with minimal configuration
    const testResources = await connectionTestHelpers.setupFakerConnectionTest(
      request,
      workspaceId,
      "Faker source",
      "E2E Testing destination"
    );

    source = testResources.source;
    destination = testResources.destination;
  });

  test.afterEach(async ({ request }) => {
    await connectionTestHelpers.cleanupConnectionTest(request, {
      connectionId: connectionId || undefined,
      sourceId: source.sourceId,
      destinationId: destination.destinationId,
    });
  });

  test("should initialize as pending", async ({ page, request }) => {
    const connection = await connectionAPI.create(request, source, destination);
    connectionId = connection.connectionId;

    await connectionUI.visit(page, connection, "status");

    return expect(page.locator("[data-testid='connection-status-indicator']")).toHaveAttribute(
      "data-status",
      "pending",
      {
        timeout: 10000,
      }
    );
  });

  test("should allow starting a sync", async ({ page, request }) => {
    // Create connection with enabled streams for sync testing
    const connection = await connectionAPI.create(request, source, destination, { enableAllStreams: true });
    connectionId = connection.connectionId;

    await connectionUI.visit(page, connection, "status");

    // Start manual sync and verify button becomes disabled
    await connectionUI.startManualSync(page);
    await expect(page.locator("[data-testid='manual-sync-button']")).toBeDisabled({ timeout: 10000 });

    // Wait for the job to start (data-loading="true")
    await expect(page.locator("[data-testid='connection-status-indicator'][data-loading='true']")).toBeVisible({
      timeout: 30000,
    });

    // Wait for the job to complete (data-loading="false")
    await expect(page.locator("[data-testid='connection-status-indicator'][data-loading='false']")).toBeVisible({
      timeout: 120000, // Should complete in < 1 minute, but we should leave some buffer for potential slowdowns
    });

    // Verify manual sync button is enabled again
    return expect(page.locator("[data-testid='manual-sync-button']")).toBeEnabled({ timeout: 10000 });
  });
});
