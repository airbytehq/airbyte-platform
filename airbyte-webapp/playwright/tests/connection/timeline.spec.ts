import { test, expect } from "@playwright/test";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { connectionAPI, connectionUI, jobUI, connectionTestHelpers } from "../../helpers/connection";
import { pokeSourceAPI, e2eDestinationAPI } from "../../helpers/connectors";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection Timeline", () => {
  let workspaceId: string;
  let sourceId: string;
  let destinationId: string;
  let connectionId: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.beforeEach(async ({ request }) => {
    // Create source and destination for each test using helper
    const testResources = await connectionTestHelpers.setupConnectionTest(
      request,
      workspaceId,
      "PokeAPI source",
      "E2E Testing destination"
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

  test("Should list events and interact with job logs modal and links", async ({ page, request, context }) => {
    // Grant clipboard permissions for this test
    await context.grantPermissions(["clipboard-read", "clipboard-write"]);

    // Navigate to home page first
    await page.goto("/", { timeout: 10000 });

    // Get the source and destination objects that were created in test setup
    const sources = (await pokeSourceAPI.list(request, workspaceId)) as SourceRead[];
    const destinations = (await e2eDestinationAPI.list(request, workspaceId)) as DestinationRead[];

    const source = sources.find((s) => s.sourceId === sourceId);
    const destination = destinations.find((d) => d.destinationId === destinationId);

    if (!source || !destination) {
      throw new Error("Failed to find created source or destination");
    }

    // Create connection
    const connection = await connectionAPI.create(request, source, destination);
    connectionId = connection.connectionId;

    // Navigate to timeline page
    await connectionUI.visit(page, connection, "timeline");

    // Initially should show no events
    await expect(page.locator("text=No events to display")).toBeVisible({ timeout: 10000 });

    // Start a manual sync
    await connectionUI.startManualSync(page);

    // Verify sync started event appears using helper
    await connectionUI.verifySyncStatus.running(page);

    // Cancel the sync using helper
    await connectionUI.cancelSync(page);
    await connectionUI.verifySyncStatus.cancelled(page);

    // Test copying link from timeline using helpers
    await jobUI.openEventMenu(page);
    const clipboardText = await jobUI.copyEventLink(page);
    await page.goto(clipboardText, { timeout: 15000 });

    // Verify job logs modal opens and close it
    await jobUI.verifyLogsModal(page);
    await jobUI.closeLogsModal(page);

    // Navigate back to timeline
    await connectionUI.visit(page, connection, "timeline");

    // Test view logs from menu + copying link from modal using helpers
    await jobUI.openEventMenu(page);
    await jobUI.openLogsFromMenu(page);

    const attemptClipboardText = await jobUI.copyAttemptLink(page);
    await jobUI.closeLogsModal(page);

    // Navigate to the copied attempt link
    await page.goto(attemptClipboardText, { timeout: 15000 });

    // Verify job logs modal opens again and close it
    await jobUI.verifyLogsModal(page);
    await jobUI.closeLogsModal(page);
  });
});
