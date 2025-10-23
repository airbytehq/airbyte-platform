/**
 * Auto-Detect Schema Changes Tests (With Worker Isolation)
 *
 * Tests the connection's ability to automatically detect and respond to schema changes
 * in the source database, including non-breaking and breaking changes.
 *
 * PARALLELIZATION STRATEGY:
 * Each Playwright worker operates in its own PostgreSQL schema:
 * - Worker 0 uses 'test_worker_0' schema
 * - Worker 1 uses 'test_worker_1' schema
 * - Worker N uses 'test_worker_N' schema
 *
 * Benefits:
 * - Faster execution with parallelization
 * - Complete isolation from public schema (no risk of polluting other tests)
 */

import { test, expect } from "@playwright/test";
import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

import { connectionAPI, connectionTestScaffold, ConnectionTestData, connectionForm } from "../../helpers/connection";
import { connectionList } from "../../helpers/connectionList";
import { dbHelpers, alterTable, getWorkerSchema } from "../../helpers/database";
import { schemaChange, catalogDiffModal, streamsTable, replicationForm } from "../../helpers/replication";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection - Auto-detect schema changes", () => {
  let workspaceId: string;
  let schema: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.describe("No changes", () => {
    let testData: ConnectionTestData;

    test.beforeEach(async ({ request }, testInfo) => {
      // Get worker-specific schema for isolation
      schema = getWorkerSchema(testInfo.parallelIndex);
      console.log(`[Test] Using schema: ${schema} (worker ${testInfo.parallelIndex})`);

      // Setup worker schema with baseline tables
      await dbHelpers.setupWorkerSchema(schema);

      // Create a Postgres → Postgres connection with all streams enabled
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        enableAllStreams: true,
        schema, // Pass schema for connector configuration
      });
    });

    test.afterEach(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      // Teardown worker schema (drops schema and all tables)
      await dbHelpers.teardownWorkerSchema(schema);
    });

    test("shows no diff after refresh if there have been no changes", async ({ page }) => {
      // Navigate to the replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`);

      // Wait for page to load
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Set up listener for schema refresh API call
      const refreshPromise = page.waitForResponse(
        (response) =>
          response.url().includes("/web_backend/connections/get") &&
          response.request().postDataJSON()?.withRefreshedCatalog === true,
        { timeout: 30000 }
      );

      // Click refresh schema button
      await streamsTable.clickRefreshSchemaButton(page);

      await refreshPromise;

      // Verify "No diff" toast appears
      await schemaChange.verifyNoDiffToast(page);

      // Verify catalog diff modal does NOT appear
      await catalogDiffModal.verifyModalNotVisible(page);
    });
  });

  test.describe("Non-breaking changes", () => {
    let testData: ConnectionTestData;

    test.beforeEach(async ({ request }, testInfo) => {
      // Get worker-specific schema for isolation
      schema = getWorkerSchema(testInfo.parallelIndex);
      console.log(`[Test] Using schema: ${schema} (worker ${testInfo.parallelIndex})`);

      // Setup worker schema with baseline tables
      await dbHelpers.setupWorkerSchema(schema);

      // Create a Postgres → Postgres connection with all streams enabled
      // With "ignore", non-breaking changes are detected but connection stays enabled
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        enableAllStreams: true,
        nonBreakingChangesPreference: "ignore",
        schema, // Pass schema for connector configuration
      });

      // Make schema changes in the database
      await dbHelpers.makeChangesInDBSource(schema);
    });

    test.afterEach(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      // Teardown worker schema
      await dbHelpers.teardownWorkerSchema(schema);
    });

    test("does not show non-breaking change on list page", async ({ page, request }) => {
      // Refresh schema via API to detect changes
      // This is needed for the list page test since we can't click refresh there
      await connectionAPI.refreshSchema(request, testData.connection.connectionId);

      // Navigate to connections list
      await connectionList.visit(page, workspaceId);

      // Verify NO warning icon appears (non-breaking changes with "ignore" don't show on list)
      await connectionList.verifyNoWarningIcon(page, testData.connection);

      // Verify connection is still enabled
      await connectionList.verifyConnectionEnabled(page, testData.connection);
    });

    test("shows non-breaking change that can be saved after refresh", async ({ page, request }) => {
      // Refresh schema via API first (matching Cypress: done before navigating to page)
      // This ensures the banner is visible when the page loads
      testData.connection = await connectionAPI.refreshSchema(request, testData.connection.connectionId);

      // Navigate to the replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`);

      // Wait for page to load and verify refresh button is visible
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Verify the schema change banner is visible (non-breaking changes)
      await schemaChange.verifySchemaChangeBanner(page, false);

      // Verify connection sync switch is enabled (non-breaking doesn't disable)
      const syncSwitch = page.locator('[data-testid="connection-status-switch"]');
      await expect(syncSwitch).toBeEnabled();

      // Click the "Review changes" button on the banner to open the catalog diff modal
      await schemaChange.clickReviewChangesButton(page);

      // Verify catalog diff modal is visible
      await catalogDiffModal.verifyModalVisible(page);

      // Verify removed streams (cities table was dropped)
      await catalogDiffModal.verifyRemovedStreams(page, ["cities"]);

      // Verify new streams (cars table was created)
      await catalogDiffModal.verifyNewStreams(page, ["cars"]);

      // Toggle users stream accordion to see field changes
      await catalogDiffModal.toggleStreamAccordion(page, "users");

      // Verify removed fields (email was dropped)
      await catalogDiffModal.verifyRemovedFields(page, ["email"]);

      // Verify new fields (phone and address were added)
      await catalogDiffModal.verifyNewFields(page, ["address", "phone"]);

      // Close the modal
      await catalogDiffModal.closeModal(page);

      // Verify banner is cleared after closing modal
      await schemaChange.verifySchemaChangeBannerCleared(page);

      // Save changes (no reset modal expected for non-breaking changes)
      await replicationForm.saveChanges(page, false);

      // Verify connection is still enabled after save
      await expect(syncSwitch).toBeEnabled();
    });

    test("clears non-breaking change when db changes are restored", async ({ page, request }) => {
      // Refresh schema via API first
      // This ensures the banner is visible when the page loads
      testData.connection = await connectionAPI.refreshSchema(request, testData.connection.connectionId);

      // Navigate to the replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`);

      // Wait for page to load and verify the schema change banner is visible
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Verify the schema change banner is visible (from the API refresh in beforeEach)
      await schemaChange.verifySchemaChangeBanner(page, false);

      // Reverse the database changes (restore cities table, revert users field changes, drop cars)
      await dbHelpers.reverseChangesInDBSource(schema);

      // Click the "Review changes" button on the banner (this triggers another schema refresh)
      await schemaChange.clickReviewChangesButton(page);

      // After clicking review, the banner should clear because there are no changes anymore
      await schemaChange.verifySchemaChangeBannerCleared(page);

      // Verify catalog diff modal does NOT appear (no changes to show)
      await catalogDiffModal.verifyModalNotVisible(page);

      // Verify "No diff" toast appears
      await schemaChange.verifyNoDiffToast(page);
    });
  });

  test.describe("Breaking changes", () => {
    let testData: ConnectionTestData;

    test.beforeEach(async ({ request }, testInfo) => {
      // Get worker-specific schema for isolation
      schema = getWorkerSchema(testInfo.parallelIndex);
      console.log(`[Test] Using schema: ${schema} (worker ${testInfo.parallelIndex})`);

      // Setup worker schema with baseline tables
      await dbHelpers.setupWorkerSchema(schema);

      // Create a Postgres → Postgres connection with all streams enabled
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        enableAllStreams: true,
        schema, // Pass schema for connector configuration
      });

      // Configure the users stream to use incremental sync with cursor field
      // This sets up the scenario where dropping the cursor field will cause a breaking change
      const streamToUpdate = testData.connection.syncCatalog.streams.findIndex(
        (stream) => stream.stream?.name === "users" && stream.stream?.namespace === schema
      );

      if (streamToUpdate === -1) {
        throw new Error("Users stream not found in connection");
      }

      const updatedCatalog = { streams: [...testData.connection.syncCatalog.streams] };
      updatedCatalog.streams[streamToUpdate].config = {
        ...updatedCatalog.streams[streamToUpdate].config,
        destinationSyncMode: DestinationSyncMode.append_dedup,
        syncMode: SyncMode.incremental,
        cursorField: ["updated_at"],
      };

      // Update the connection with the new sync catalog
      testData.connection = await connectionAPI.update(request, testData.connection.connectionId, {
        syncCatalog: updatedCatalog,
      });

      // Drop the cursor field from the database to create a breaking change
      await dbHelpers.runQuery(alterTable("users", { drop: ["updated_at"] }, schema));

      // Refresh the schema via API
      testData.connection = await connectionAPI.refreshSchema(request, testData.connection.connectionId);
    });

    test.afterEach(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      // Teardown worker schema
      await dbHelpers.teardownWorkerSchema(schema);
    });

    test("shows breaking change on list page", async ({ page }) => {
      // Navigate to connections list
      await connectionList.visit(page, workspaceId);

      // Verify error icon appears (breaking changes show as errors)
      await connectionList.verifyHasErrorIcon(page, testData.connection);

      // Verify connection is disabled (breaking changes auto-disable the connection)
      await connectionList.verifyConnectionDisabled(page, testData.connection);
    });

    test("shows breaking change that can be saved after refresh and fix", async ({ page, request }) => {
      // Navigate to the replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`);

      // Wait for page to load
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Verify the breaking change banner is visible
      await schemaChange.verifySchemaChangeBanner(page, true);

      // Verify connection sync switch is disabled (breaking changes disable the connection)
      const syncSwitch = page.locator('[data-testid="connection-status-switch"]');
      await expect(syncSwitch).toBeDisabled();

      // Click the "Review changes" button on the banner to open the catalog diff modal
      await schemaChange.clickReviewChangesButton(page);

      // Verify the sync switch is still disabled after opening the modal
      await expect(syncSwitch).toBeDisabled();

      // Verify catalog diff modal is visible
      await catalogDiffModal.verifyModalVisible(page);

      // Verify no removed or new streams (only field changes)
      await catalogDiffModal.verifyNoRemovedStreams(page);
      await catalogDiffModal.verifyNoNewStreams(page);

      // Toggle users stream accordion to see field changes
      await catalogDiffModal.toggleStreamAccordion(page, "users");

      // Verify removed field (updated_at was dropped)
      await catalogDiffModal.verifyRemovedFields(page, ["updated_at"]);

      // Verify no new fields
      await catalogDiffModal.verifyNoNewFields(page);

      // Close the modal
      await catalogDiffModal.closeModal(page);

      // Verify banner is cleared after closing modal
      await schemaChange.verifySchemaChangeBannerCleared(page);

      // Fix the conflict by changing the sync mode to full_refresh
      await streamsTable.filterByStreamName(page, "users");
      await streamsTable.selectSyncMode(page, schema, "users", SyncMode.full_refresh, DestinationSyncMode.append);

      // Save changes (no reset modal expected for fixing breaking changes)
      await replicationForm.saveChanges(page, false);

      // Verify connection is now enabled after fixing the breaking change
      await expect(syncSwitch).toBeEnabled();
    });

    test("clears breaking change if db changes are restored", async ({ page, request }) => {
      // Navigate to the replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`);

      // Wait for page to load
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Verify the breaking change banner is visible
      await schemaChange.verifySchemaChangeBanner(page, true);

      // Restore the dropped column in the database
      await dbHelpers.runQuery(alterTable("users", { add: ["updated_at TIMESTAMP"] }, schema));
      await schemaChange.clickReviewChangesButton(page);

      // After clicking review, the banner should clear because the breaking change is fixed
      await schemaChange.verifySchemaChangeBannerCleared(page);

      // Verify catalog diff modal does NOT appear (no changes to show)
      await catalogDiffModal.verifyModalNotVisible(page);

      // Verify "No diff" toast appears
      await schemaChange.verifyNoDiffToast(page);
    });
  });

  test.describe("Non-breaking schema update preference", () => {
    let testData: ConnectionTestData;

    test.beforeEach(async ({ request }, testInfo) => {
      // Get worker-specific schema for isolation
      schema = getWorkerSchema(testInfo.parallelIndex);
      console.log(`[Test] Using schema: ${schema} (worker ${testInfo.parallelIndex})`);

      // Setup worker schema with baseline tables
      await dbHelpers.setupWorkerSchema(schema);

      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        enableAllStreams: true,
        schema, // Pass schema for connector configuration
      });
    });

    test.afterEach(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      // Teardown worker schema
      await dbHelpers.teardownWorkerSchema(schema);
    });

    test("saves non-breaking schema update preference change", async ({ page }) => {
      // Navigate to the connection settings page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/settings`);
      await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible({ timeout: 10000 });

      await connectionForm.toggleAdvancedSettings(page);
      await replicationForm.selectNonBreakingChangesPreference(page, "disable");

      // Set up listener for the update API call
      const updatePromise = page.waitForResponse(
        (response) =>
          response.url().includes("/web_backend/connections/update") && response.request().method() === "POST",
        { timeout: 30000 }
      );

      await page.getByRole("button", { name: "Save changes" }).click();

      // Wait for the API call and verify the request body
      const updateResponse = await updatePromise;
      const requestBody = updateResponse.request().postDataJSON();
      expect(requestBody.nonBreakingChangesPreference).toBe("disable");

      expect(updateResponse.status()).toBe(200);

      // Verify the response body has the updated preference and success notification appears
      const responseBody = await updateResponse.json();
      expect(responseBody.nonBreakingChangesPreference).toBe("disable");
      await expect(page.getByText("Your changes were saved!")).toBeVisible();
    });
  });
});
