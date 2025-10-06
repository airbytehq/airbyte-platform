import { test, expect } from "@playwright/test";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { connectionAPI, connectionUI, connectionTestHelpers } from "../../helpers/connection";
import { postgresSourceAPI, postgresDestinationAPI } from "../../helpers/connectors";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection Status", () => {
  // Uses environment-aware Postgres setup following dummy API pattern:
  // - CI: Uses POSTGRES_TEST_HOST environment variable to connect to deployed postgres pods
  // - Local: Falls back to platform-specific docker networking (host.docker.internal/172.17.0.1)
  let workspaceId: string;
  let sourceId: string;
  let destinationId: string;
  let connectionId: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.beforeEach(async ({ request }) => {
    // Create source and destination for each test using helper
    // Uses environment-aware Postgres setup (CI: postgres pods, Local: fallback to docker/localhost)
    const testResources = await connectionTestHelpers.setupPostgresConnectionTest(
      request,
      workspaceId,
      "Postgres source",
      "Postgres destination"
    );

    sourceId = testResources.sourceId;
    destinationId = testResources.destinationId;
  });

  test.afterEach(async ({ request }) => {
    // Clean up resources using helper
    await connectionTestHelpers.cleanupConnectionTest(request, {
      connectionId: connectionId || undefined,
      sourceId,
      destinationId,
    });

    // Reset connection ID for next test
    connectionId = "";
  });

  test("should initialize as pending", async ({ page, request }) => {
    // Get the source and destination objects that were created in test setup
    const sources = (await postgresSourceAPI.list(request, workspaceId)) as SourceRead[];
    const destinations = (await postgresDestinationAPI.list(request, workspaceId)) as DestinationRead[];

    const source = sources.find((s) => s.sourceId === sourceId);
    const destination = destinations.find((d) => d.destinationId === destinationId);

    if (!source || !destination) {
      throw new Error("Failed to find created source or destination");
    }

    // Create connection
    const connection = await connectionAPI.create(request, source, destination);
    connectionId = connection.connectionId;

    // Navigate to status page with explicit timeout for CI stability
    await connectionUI.visit(page, connection, "status");

    // Verify connection status is pending
    await expect(page.locator("[data-testid='connection-status-indicator']")).toHaveAttribute(
      "data-status",
      "pending",
      {
        timeout: 10000,
      }
    );
  });

  test("should allow starting a sync", async ({ page, request }) => {
    // Get the source and destination objects that were created in test setup
    const sources = (await postgresSourceAPI.list(request, workspaceId)) as SourceRead[];
    const destinations = (await postgresDestinationAPI.list(request, workspaceId)) as DestinationRead[];

    const source = sources.find((s) => s.sourceId === sourceId);
    const destination = destinations.find((d) => d.destinationId === destinationId);

    if (!source || !destination) {
      throw new Error("Failed to find created source or destination");
    }

    // Create connection with enabled streams for sync testing
    const connection = await connectionAPI.create(request, source, destination, { enableAllStreams: true });
    connectionId = connection.connectionId;

    // Navigate to status page with explicit timeout for CI stability
    await connectionUI.visit(page, connection, "status");

    // Start manual sync and verify button becomes disabled
    await connectionUI.startManualSync(page);
    await expect(page.locator("[data-testid='manual-sync-button']")).toBeDisabled({ timeout: 10000 });

    // Wait for the job to start (data-loading="true")
    await expect(page.locator("[data-testid='connection-status-indicator'][data-loading='true']")).toBeVisible({
      timeout: 30000, // Increased timeout - sync might take time to start
    });

    // Wait for the job to complete (data-loading="false")
    await expect(page.locator("[data-testid='connection-status-indicator'][data-loading='false']")).toBeVisible({
      timeout: 120000, // Longer timeout for reliable Postgres sync
    });

    // Verify manual sync button is enabled again
    await expect(page.locator("[data-testid='manual-sync-button']")).toBeEnabled({ timeout: 10000 });
  });
});
