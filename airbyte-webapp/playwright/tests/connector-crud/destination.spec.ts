import { test, expect } from "@playwright/test";

import { destinationUI, e2eDestinationAPI } from "../../helpers/connectors";
import { mockHelpers } from "../../helpers/mocks";
import { appendRandomString, verifyUpdateSuccess, performSecondTextEdit } from "../../helpers/ui";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Destination CRUD operations", () => {
  let workspaceId: string;
  const createdDestinationIds: string[] = [];

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.afterAll(async ({ request }) => {
    // Clean up only destinations created by this worker
    if (createdDestinationIds.length > 0) {
      // Small delay to allow trace/screenshot generation to complete
      await new Promise((resolve) => setTimeout(resolve, 1000));

      for (const destinationId of createdDestinationIds) {
        try {
          await e2eDestinationAPI.delete(request, destinationId);
        } catch (error) {
          // Just warn on cleanup errors to avoid test failures
          console.warn(`⚠️ Failed to clean up destination ${destinationId}:`, error);
        }
      }
    }
  });

  test("Redirects from destination list to create page if no destinations are configured", async ({ page }) => {
    await mockHelpers.mockEmptyConnectorLists(page, "destination");
    await page.goto(`/workspaces/${workspaceId}/destination`, { waitUntil: "networkidle" });
    await expect(page).toHaveURL(/.*\/destination\/new-destination/, { timeout: 15000 });
  });

  test("Can create new destination", async ({ page }) => {
    const destinationName = appendRandomString("E2E Test destination playwright");

    // Create destination via UI
    const destinationId = await destinationUI.createViaUI(page, destinationName, workspaceId);
    createdDestinationIds.push(destinationId); // Track for cleanup

    // Verify we're on the destination detail page
    await expect(page).toHaveURL(/.*\/destination\/[a-f0-9-]{36}/, { timeout: 15000 });
  });

  test("Can update configured destination", async ({ page, request }) => {
    const destinationName = appendRandomString("End-to-End Testing (/dev/null) Destination");
    const destination = await e2eDestinationAPI.create(request, destinationName, workspaceId);
    createdDestinationIds.push(destination.destinationId); // Track for cleanup

    await destinationUI.updateDestination(
      page,
      destinationName,
      "connectionConfiguration.test_destination.logging_config.max_entry_count",
      "10",
      workspaceId
    );

    await verifyUpdateSuccess(page, "10");
  });

  test("Can edit destination again without leaving the page", async ({ page, request }) => {
    const destinationName = appendRandomString("End-to-End Testing (/dev/null) Destination");
    const destination = await e2eDestinationAPI.create(request, destinationName, workspaceId);
    createdDestinationIds.push(destination.destinationId); // Track for cleanup

    await destinationUI.updateDestination(
      page,
      destinationName,
      "connectionConfiguration.test_destination.logging_config.max_entry_count",
      "10",
      workspaceId
    );

    await verifyUpdateSuccess(page, "10");
    await expect(page.locator("button[type=submit]")).toBeDisabled({ timeout: 10000 });

    await performSecondTextEdit(page, "connectionConfiguration.test_destination.logging_config.max_entry_count", "20");
  });

  test("Can delete configured destination", async ({ page, request }) => {
    const destinationName = appendRandomString("End-to-End Testing (/dev/null) Destination");
    const destination = await e2eDestinationAPI.create(request, destinationName, workspaceId);
    createdDestinationIds.push(destination.destinationId); // Track for cleanup initially

    // Delete via UI
    await destinationUI.deleteDestination(page, destinationName, workspaceId);

    // Remove from cleanup list since we deleted it via UI
    const index = createdDestinationIds.indexOf(destination.destinationId);
    if (index > -1) {
      createdDestinationIds.splice(index, 1);
    }

    // After deletion, should redirect to either list page or new-destination page
    await expect(page).toHaveURL(/.*\/destination(\/new-destination)?$/, { timeout: 15000 });

    // Verify deletion was successful via API (works regardless of UI state)
    const remainingDestinations = await e2eDestinationAPI.list(request, workspaceId);
    const stillExists = remainingDestinations.some(
      (d: { name: string; destinationId: string }) => d.destinationId === destination.destinationId
    );
    expect(stillExists).toBe(false);
  });
});
