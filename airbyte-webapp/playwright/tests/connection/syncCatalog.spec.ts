/**
 * Sync Catalog Tests
 *
 * Tests the sync catalog UI for configuring stream replication, including:
 * - Stream and field selection
 */

import { test, expect, Page, BrowserContext, APIRequestContext } from "@playwright/test";
import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

import { connectionTestScaffold, ConnectionTestData, connectionAPI } from "../../helpers/connection";
import { postgresSourceAPI, postgresDestinationAPI } from "../../helpers/connectors";
import { streamsTable, replicationForm, navigateToReplicationPage } from "../../helpers/replication";
import { streamRow } from "../../helpers/streamRow";
import { setupWorkspaceForTests } from "../../helpers/workspace";
import { setFeatureFlags, setFeatureServiceFlags, injectFeatureFlagsAndStyle } from "../../support/e2e";

// Extended test data type that includes shared browser resources for serial tests
interface SerialTestData extends ConnectionTestData {
  page: Page;
  context: BrowserContext;
  request: APIRequestContext;
}

// Global workspace ID setup
let workspaceId: string;

test.beforeAll(async () => {
  workspaceId = await setupWorkspaceForTests();
});

test.describe.serial("Sync Catalog - Stream state tests", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    // Create connection once for all tests in this block
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Create Postgres → Postgres connection with all streams enabled
    // Uses existing tables from postgres-test-data.sql (users, cities, cars)
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
      enableAllStreams: true,
      useMockSchemaDiscovery: true,
    });

    // Navigate to replication page once for all tests
    await navigateToReplicationPage(page, workspaceId, baseTestData.connection.connectionId);

    // Combine base test data with browser resources
    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    // Cleanup connection and close browser context
    await connectionTestScaffold.cleanupConnection(testData.request, testData);
    await testData.page.close();
    await testData.context.close();
  });

  test.describe("enabled state", () => {
    test("should have checked checkbox", async () => {
      // Verify stream sync checkbox is checked (all streams enabled in setup)
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cars");
      expect(isEnabled).toBe(true);
    });

    test("should have all fields with checked checkbox", async () => {
      // Expand the stream to see fields (stays expanded for subsequent tests)
      await streamRow.toggleExpandCollapse(testData.page, "public", "cars");

      // Verify all fields have checked checkboxes
      const colorEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "color");
      expect(colorEnabled).toBe(true);

      const idEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "id");
      expect(idEnabled).toBe(true);

      const markEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "mark");
      expect(markEnabled).toBe(true);

      const modelEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "model");
      expect(modelEnabled).toBe(true);
    });

    test("should show selected SyncMode dropdown", async () => {
      // Select a sync mode (stream already expanded from previous test)
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cars",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );

      // Verify the selected sync mode is displayed
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "cars",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );
    });

    test("should show selected PK", async () => {
      // Verify the PK is displayed (auto-selected for incremental/append_dedup)
      await streamRow.verifySelectedPK(testData.page, "public", "cars", "id");
    });

    test("should show cursor error", async () => {
      // Verify cursor error is displayed (no cursor field selected yet)
      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "cars");
      expect(hasCursorError).toBe(true);
    });
  });

  test.describe("disabled state", () => {
    // Continue from enabled state tests
    test("should have unchecked checkbox", async () => {
      // Toggle stream sync off
      await streamRow.toggleStreamSync(testData.page, "public", "cars", false);

      // Verify stream sync checkbox is now unchecked
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cars");
      expect(isEnabled).toBe(false);
    });

    test("should have all fields with unchecked checkbox", async () => {
      // Verify all field checkboxes are now unchecked
      const colorEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "color");
      expect(colorEnabled).toBe(false);

      const idEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "id");
      expect(idEnabled).toBe(false);

      const markEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "mark");
      expect(markEnabled).toBe(false);

      const modelEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "model");
      expect(modelEnabled).toBe(false);
    });

    test("should not show sync mode dropdown", async () => {
      // Verify sync mode dropdown is hidden when stream is disabled
      const isDisplayed = await streamRow.isSyncModeDropdownDisplayed(testData.page, "public", "cars");
      expect(isDisplayed).toBe(false);
    });

    test("should not show PK combobox", async () => {
      // Verify PK combobox is hidden when stream is disabled
      const isDisplayed = await streamRow.isPKComboboxDisplayed(testData.page, "public", "cars");
      expect(isDisplayed).toBe(false);
    });

    test("should not show cursor combobox", async () => {
      // Verify cursor combobox is hidden when stream is disabled
      const isDisplayed = await streamRow.isCursorComboboxDisplayed(testData.page, "public", "cars");
      expect(isDisplayed).toBe(false);

      // Collapse the stream for cleanup
      await streamRow.toggleExpandCollapse(testData.page, "public", "cars");
    });
  });

  test.describe("field operations", () => {
    // Continue from disabled state tests
    test("should show field checkbox by default", async () => {
      // Expand stream to access field-level controls
      await streamRow.toggleExpandCollapse(testData.page, "public", "cars");

      // Enable the stream
      await streamRow.toggleStreamSync(testData.page, "public", "cars", true);

      // Verify field checkbox is displayed when stream is enabled
      const displayedWhenEnabled = await streamRow.isFieldSyncCheckboxDisplayed(
        testData.page,
        "public",
        "cars",
        "color"
      );
      expect(displayedWhenEnabled).toBe(true);

      // Disable the stream
      await streamRow.toggleStreamSync(testData.page, "public", "cars", false);

      // Verify field checkbox is still displayed when stream is disabled
      const displayedWhenDisabled = await streamRow.isFieldSyncCheckboxDisplayed(
        testData.page,
        "public",
        "cars",
        "color"
      );
      expect(displayedWhenDisabled).toBe(true);

      // Re-enable the stream for subsequent tests
      await streamRow.toggleStreamSync(testData.page, "public", "cars", true);
    });

    test("should show field type", async () => {
      // Verify field types are displayed correctly
      await streamRow.verifyFieldType(testData.page, "public", "cars", "id", "Integer");
      await streamRow.verifyFieldType(testData.page, "public", "cars", "color", "String");
    });

    test("should have disabled checkbox if field is a PK or Cursor", async () => {
      // Select incremental/append_dedup sync mode (requires PK and cursor)
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cars",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );

      // Select cursor field
      await streamRow.selectCursor(testData.page, "public", "cars", "mark");

      // Verify PK field (id) is enabled but checkbox is disabled (required field)
      const idEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "id");
      expect(idEnabled).toBe(true);

      const idCheckboxDisabled = await streamRow.isFieldSyncCheckboxDisabled(testData.page, "public", "cars", "id");
      expect(idCheckboxDisabled).toBe(true);

      // Verify cursor field (mark) is enabled but checkbox is disabled (required field)
      const markEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "mark");
      expect(markEnabled).toBe(true);

      const markCheckboxDisabled = await streamRow.isFieldSyncCheckboxDisabled(testData.page, "public", "cars", "mark");
      expect(markCheckboxDisabled).toBe(true);
    });

    test("should show PK and Cursor labels", async () => {
      // Verify PK label is displayed for id field
      const idIsPK = await streamRow.isFieldPK(testData.page, "public", "cars", "id");
      expect(idIsPK).toBe(true);

      // Verify cursor label is displayed for mark field
      const markIsCursor = await streamRow.isFieldCursor(testData.page, "public", "cars", "mark");
      expect(markIsCursor).toBe(true);
    });

    test("should enable the stream and required fields", async () => {
      // Disable the stream
      await streamRow.toggleStreamSync(testData.page, "public", "cars", false);

      // Verify stream is disabled
      const streamDisabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cars");
      expect(streamDisabled).toBe(false);

      // Enable a single field (color) - this should auto-enable the stream and required fields
      await streamRow.toggleFieldSync(testData.page, "public", "cars", "color", true);

      // Verify the selected field (color) is enabled
      const colorEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "color");
      expect(colorEnabled).toBe(true);

      // Verify required PK field (id) is auto-enabled and disabled
      const idEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "id");
      expect(idEnabled).toBe(true);

      const idCheckboxDisabled = await streamRow.isFieldSyncCheckboxDisabled(testData.page, "public", "cars", "id");
      expect(idCheckboxDisabled).toBe(true);

      // Verify required cursor field (mark) is auto-enabled and disabled
      const markEnabled = await streamRow.isFieldSyncEnabled(testData.page, "public", "cars", "mark");
      expect(markEnabled).toBe(true);

      const markCheckboxDisabled = await streamRow.isFieldSyncCheckboxDisabled(testData.page, "public", "cars", "mark");
      expect(markCheckboxDisabled).toBe(true);
    });

    test("should disable the stream if all fields are unselected", async () => {
      // Change to full_refresh/append (no required fields)
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cars",
        SyncMode.full_refresh,
        DestinationSyncMode.append
      );

      // Unselect all fields one by one
      await streamRow.toggleFieldSync(testData.page, "public", "cars", "color", false);
      await streamRow.toggleFieldSync(testData.page, "public", "cars", "id", false);
      await streamRow.toggleFieldSync(testData.page, "public", "cars", "mark", false);

      // Verify stream is auto-disabled when all fields are unselected
      const isStreamEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cars");
      expect(isStreamEnabled).toBe(false);
    });
  });
});

/**
 * Feature Flag Tests
 *
 * These tests require feature flag injection and cannot share a page with other tests.
 * Each feature flag test needs its own connection and browser context with flags injected.
 */
test.describe.serial("Sync Catalog - Feature flag tests", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    // Set feature flags to disable columnSelection
    setFeatureFlags({ "connection.columnSelection": false });
    setFeatureServiceFlags({});

    // Create a fresh browser context and page
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Inject feature flags into the page before navigation
    await injectFeatureFlagsAndStyle(page);

    // Create connection with all streams enabled
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
      enableAllStreams: true,
      useMockSchemaDiscovery: true,
    });

    // Navigate to replication page
    await navigateToReplicationPage(page, workspaceId, baseTestData.connection.connectionId);

    // Combine base test data with browser resources
    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    // Clear feature flags
    setFeatureFlags({});
    setFeatureServiceFlags({});

    // Cleanup connection and close browser context
    await connectionTestScaffold.cleanupConnection(testData.request, testData);
    await testData.page.close();
    await testData.context.close();
  });

  test("should not show field sync checkbox when columnSelection feature is disabled", async () => {
    // Enable the stream
    await streamRow.toggleStreamSync(testData.page, "public", "cars", true);

    // Select incremental/append_dedup sync mode
    await streamRow.selectSyncMode(
      testData.page,
      "public",
      "cars",
      SyncMode.incremental,
      DestinationSyncMode.append_dedup
    );

    // Select cursor field
    await streamRow.selectCursor(testData.page, "public", "cars", "mark");

    // Expand the stream to see fields
    await streamRow.toggleExpandCollapse(testData.page, "public", "cars");

    // Verify field checkboxes are NOT displayed when columnSelection is disabled
    const idCheckboxDisplayed = await streamRow.isFieldSyncCheckboxDisplayed(testData.page, "public", "cars", "id");
    expect(idCheckboxDisplayed).toBe(false);

    const markCheckboxDisplayed = await streamRow.isFieldSyncCheckboxDisplayed(testData.page, "public", "cars", "mark");
    expect(markCheckboxDisplayed).toBe(false);

    const colorCheckboxDisplayed = await streamRow.isFieldSyncCheckboxDisplayed(
      testData.page,
      "public",
      "cars",
      "color"
    );
    expect(colorCheckboxDisplayed).toBe(false);
  });
});

/**
 * Sync Modes Tests
 *
 * Tests all combinations of sync modes (Full Refresh, Incremental) with destination modes
 * (Append, Overwrite, Overwrite+Deduped, Append+Deduped). Each test suite:
 * 1. Configures a stream with the sync mode
 * 2. Verifies PK/Cursor controls visibility and error states
 * 3. Saves the configuration
 * 4. Reloads the page to verify persistence
 *
 * Uses 2 streams: users (with source-defined PK) and cities (without source-defined PK)
 */
test.describe.serial("Sync Catalog - Sync Modes", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    // Create connection once for all sync mode tests
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Create Postgres → Postgres connection (no streams enabled by default)
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
      useMockSchemaDiscovery: true,
    });

    // Navigate to replication page
    await navigateToReplicationPage(page, workspaceId, baseTestData.connection.connectionId);

    // Combine base test data with browser resources
    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    // Cleanup connection and close browser context
    await connectionTestScaffold.cleanupConnection(testData.request, testData);
    await testData.page.close();
    await testData.context.close();
  });

  test.describe("Full refresh | Append", () => {
    test("should select the sync mode", async () => {
      // No reload needed - page already loaded in beforeAll
      // Enable users stream and select Full Refresh | Append
      await streamRow.toggleStreamSync(testData.page, "public", "users", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.append
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.append
      );
    });

    test("should not display the PK and Cursor combobox buttons", async () => {
      // Full Refresh | Append doesn't require PK or cursor
      const isPKDisplayed = await streamRow.isPKComboboxDisplayed(testData.page, "public", "users");
      expect(isPKDisplayed).toBe(false);

      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "users");
      expect(hasPKError).toBe(false);

      const isCursorDisplayed = await streamRow.isCursorComboboxDisplayed(testData.page, "public", "users");
      expect(isCursorDisplayed).toBe(false);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "users");
      expect(hasCursorError).toBe(false);
    });

    test("should allow to save changes", async () => {
      // Verify no error state and save
      const hasNoStreamsError = await streamsTable.hasNoStreamsSelectedError(testData.page);
      expect(hasNoStreamsError).toBe(false);

      await streamsTable.clickSaveChangesButton(testData.page);

      // Wait for success notification
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.append
      );
    });
  });

  test.describe("Full refresh | Overwrite", () => {
    test("should select the sync mode", async () => {
      // No reload needed - previous test already verified persistence
      // Just select new sync mode (stream already enabled from previous test)
      await streamRow.toggleStreamSync(testData.page, "public", "users", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );
    });

    test("should not display the PK and Cursor combobox buttons", async () => {
      // Full Refresh | Overwrite doesn't require PK or cursor
      const isPKDisplayed = await streamRow.isPKComboboxDisplayed(testData.page, "public", "users");
      expect(isPKDisplayed).toBe(false);

      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "users");
      expect(hasPKError).toBe(false);

      const isCursorDisplayed = await streamRow.isCursorComboboxDisplayed(testData.page, "public", "users");
      expect(isCursorDisplayed).toBe(false);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "users");
      expect(hasCursorError).toBe(false);
    });

    test("should allow to save changes", async () => {
      // Verify no error state and save
      const hasNoStreamsError = await streamsTable.hasNoStreamsSelectedError(testData.page);
      expect(hasNoStreamsError).toBe(false);

      await streamsTable.clickSaveChangesButton(testData.page);

      // Wait for success notification
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );
    });
  });

  test.describe("Full refresh | Overwrite + Deduped", () => {
    test("should select the sync mode", async () => {
      // No reload needed - previous test already verified persistence
      // Enable cities stream (different stream from previous tests)
      await streamRow.toggleStreamSync(testData.page, "public", "cities", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite_dedup
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite_dedup
      );
    });

    test("should show missing PK error", async () => {
      // Overwrite + Deduped requires PK but not cursor
      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "cities");
      expect(hasPKError).toBe(true);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "cities");
      expect(hasCursorError).toBe(false);

      // Save button should be disabled due to missing PK
      const isSaveEnabled = await streamsTable.isSaveChangesButtonEnabled(testData.page);
      expect(isSaveEnabled).toBe(false);
    });

    test("should select PK", async () => {
      // Select city_code as PK
      await streamRow.selectPKs(testData.page, "public", "cities", ["city_code"]);
      await streamRow.verifySelectedPK(testData.page, "public", "cities", "city_code");

      // Verify PK error is gone
      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "cities");
      expect(hasPKError).toBe(false);

      // Expand stream to verify PK label on field
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");
      const isFieldPK = await streamRow.isFieldPK(testData.page, "public", "cities", "city_code");
      expect(isFieldPK).toBe(true);
    });

    test("should allow to save changes", async () => {
      await streamsTable.clickSaveChangesButton(testData.page);

      // Wait for success notification
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should select multiple PKs", async () => {
      // Reload page to verify persisted state before adding another PK
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Add city as additional PK (city_code is already selected from previous save)
      await streamRow.selectPKs(testData.page, "public", "cities", ["city"]);
      await streamRow.verifySelectedPK(testData.page, "public", "cities", "2 items selected");

      // Expand stream to verify both PK labels
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");
      const isCityCodePK = await streamRow.isFieldPK(testData.page, "public", "cities", "city_code");
      expect(isCityCodePK).toBe(true);
      const isCityPK = await streamRow.isFieldPK(testData.page, "public", "cities", "city");
      expect(isCityPK).toBe(true);

      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode and PKs
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedPK(testData.page, "public", "cities", "2 items selected");

      // Note: Sync mode display changes from overwrite_dedup to overwrite after save
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );
    });
  });

  test.describe("Full refresh | Overwrite + Deduped (source-defined PK)", () => {
    test("should select the sync mode", async () => {
      // No reload needed - previous test already verified persistence
      // Switch to users stream and select Full Refresh | Overwrite + Deduped
      await streamRow.toggleStreamSync(testData.page, "public", "users", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite_dedup
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite_dedup
      );
    });

    test("should NOT show missing PK and Cursor error", async () => {
      // Source-defined PK is automatically used, no error
      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "users");
      expect(hasPKError).toBe(false);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "users");
      expect(hasCursorError).toBe(false);

      // Save button should be enabled
      const isSaveEnabled = await streamsTable.isSaveChangesButtonEnabled(testData.page);
      expect(isSaveEnabled).toBe(true);
    });

    test("should show non-editable selected PK", async () => {
      // Verify source-defined PK is shown and not editable
      await streamRow.verifySelectedPK(testData.page, "public", "users", "id");

      const isPKDisabled = await streamRow.isPKComboboxDisabled(testData.page, "public", "users");
      expect(isPKDisabled).toBe(true);

      // Expand stream to verify PK label on field
      await streamRow.toggleExpandCollapse(testData.page, "public", "users");
      const isFieldPK = await streamRow.isFieldPK(testData.page, "public", "users", "id");
      expect(isFieldPK).toBe(true);
    });

    test("should allow to save changes", async () => {
      await streamsTable.clickSaveChangesButton(testData.page);

      // Wait for success notification
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode and PK
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedPK(testData.page, "public", "users", "id");

      // Note: Sync mode display changes from overwrite_dedup to overwrite after save
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );
    });
  });

  test.describe("Incremental | Append", () => {
    test("should select the sync mode", async () => {
      // No reload needed - previous test already verified persistence
      // Just select new sync mode (stream already enabled)
      await streamRow.toggleStreamSync(testData.page, "public", "users", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.incremental,
        DestinationSyncMode.append
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.incremental,
        DestinationSyncMode.append
      );
    });

    test("should show missing Cursor error", async () => {
      // Incremental requires cursor but not PK
      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "users");
      expect(hasPKError).toBe(false);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "users");
      expect(hasCursorError).toBe(true);

      // Save button should be disabled due to missing cursor
      const isSaveEnabled = await streamsTable.isSaveChangesButtonEnabled(testData.page);
      expect(isSaveEnabled).toBe(false);
    });

    test("should select Cursor", async () => {
      // Select email as cursor
      await streamRow.selectCursor(testData.page, "public", "users", "email");
      await streamRow.verifySelectedCursor(testData.page, "public", "users", "email");

      // Expand stream to verify cursor label on field
      await streamRow.toggleExpandCollapse(testData.page, "public", "users");
      const isFieldCursor = await streamRow.isFieldCursor(testData.page, "public", "users", "email");
      expect(isFieldCursor).toBe(true);
    });

    test("should allow to save changes", async () => {
      await streamsTable.clickSaveChangesButton(testData.page);

      // Wait for success notification
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode and cursor
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedCursor(testData.page, "public", "users", "email");

      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "users",
        SyncMode.incremental,
        DestinationSyncMode.append
      );
    });
  });

  test.describe("Incremental | Append + Deduped", () => {
    test("should select the sync mode", async () => {
      // Trick to unset PKs from previous tests (cities has 2 PKs from "Full refresh | Overwrite + Deduped" test)
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Deselect the existing PKs by re-selecting them (toggles them off)
      await streamRow.selectPKs(testData.page, "public", "cities", ["city_code", "city"]);

      // Change to a sync mode that doesn't require PKs
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );

      // Save to persist the cleared PKs
      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

      // Reload page to get clean state with no PKs
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Now enable cities stream and configure Incremental | Append + Deduped
      await streamRow.toggleStreamSync(testData.page, "public", "cities", true);
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(true);

      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );
      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );
    });

    test("should show missing PK and Cursor errors", async () => {
      // Incremental | Append + Deduped requires both PK and cursor
      const hasPKError = await streamRow.hasMissingPKError(testData.page, "public", "cities");
      expect(hasPKError).toBe(true);

      const hasCursorError = await streamRow.hasMissingCursorError(testData.page, "public", "cities");
      expect(hasCursorError).toBe(true);

      // Save button should be disabled
      const isSaveEnabled = await streamsTable.isSaveChangesButtonEnabled(testData.page);
      expect(isSaveEnabled).toBe(false);
    });

    test("should select PK and Cursor", async () => {
      // Select PK and cursor
      await streamRow.selectPKs(testData.page, "public", "cities", ["city_code"]);
      await streamRow.verifySelectedPK(testData.page, "public", "cities", "city_code");

      await streamRow.selectCursor(testData.page, "public", "cities", "city");
      await streamRow.verifySelectedCursor(testData.page, "public", "cities", "city");

      // Expand stream to verify labels
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");
      const isFieldPK = await streamRow.isFieldPK(testData.page, "public", "cities", "city_code");
      expect(isFieldPK).toBe(true);
      const isFieldCursor = await streamRow.isFieldCursor(testData.page, "public", "cities", "city");
      expect(isFieldCursor).toBe(true);
    });

    test("should allow to save changes and discard refresh streams", async () => {
      // Use replicationForm.saveChanges helper to handle reset/refresh modal
      // This modal appears when changing to Incremental | Append + Deduped
      // Unlike other tests that just save, this requires modal interaction
      await replicationForm.saveChanges(testData.page, true);
    });

    test("should verify that changes are applied", async () => {
      // Reload page to verify persistence
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify stream is still enabled with correct sync mode, PK, and cursor
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(true);

      await streamRow.verifySelectedPK(testData.page, "public", "cities", "city_code");
      await streamRow.verifySelectedCursor(testData.page, "public", "cities", "city");

      await streamRow.verifySelectedSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );
    });
  });
});

/**
 * Diff Styles Tests
 *
 * Tests visual styling of streams and fields as they change state:
 * - "added" style when enabling streams/fields
 * - "removed" style when disabling streams/fields
 * - "changed" style when modifying sync mode/PK/cursor
 * - "disabled" style when stream is disabled
 *
 * These tests verify both unsaved state styling and post-save/discard behavior.
 * Reloads are intentional to test that styles persist correctly after page refresh.
 */
test.describe.serial("Sync Catalog - Diff Styles", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    // Create connection once for all diff style tests
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Create Postgres → Postgres connection with all streams enabled by default
    // NOTE: This test suite tests diff styles which require proper backend catalog state management.
    // Mocking is intentionally disabled here as it doesn't preserve state correctly through updates.
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres");

    // Navigate to replication page
    await navigateToReplicationPage(page, workspaceId, baseTestData.connection.connectionId);

    // Combine base test data with browser resources
    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    // Cleanup connection and close browser context
    await connectionTestScaffold.cleanupConnection(testData.request, testData);
    await testData.page.close();
    await testData.context.close();
  });

  test.describe("Stream styles", () => {
    test("should have 'removed' style after changing state from enabled => disabled", async () => {
      // Streams start enabled from setupConnection - disable cities to test "removed" style
      await streamRow.toggleStreamSync(testData.page, "public", "cities", false);

      const hasRemovedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "removed");
      expect(hasRemovedStyle).toBe(true);

      // Expand to check field styles
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");

      const cityDisabled = await streamRow.hasFieldStyle(testData.page, "public", "cities", "city", "disabled");
      expect(cityDisabled).toBe(true);

      const cityCodeDisabled = await streamRow.hasFieldStyle(
        testData.page,
        "public",
        "cities",
        "city_code",
        "disabled"
      );
      expect(cityCodeDisabled).toBe(true);

      // Save - removed style should clear
      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

      const hasRemovedStyleAfterSave = await streamRow.hasStreamStyle(testData.page, "public", "cities", "removed");
      expect(hasRemovedStyleAfterSave).toBe(false);
    });

    test("should have 'disabled' style if stream is not enabled", async () => {
      // Reload page
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify cities is disabled
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(false);

      // Should have "disabled" style
      const hasDisabledStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "disabled");
      expect(hasDisabledStyle).toBe(true);

      // Expand to check field styles
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");

      const cityDisabled = await streamRow.hasFieldStyle(testData.page, "public", "cities", "city", "disabled");
      expect(cityDisabled).toBe(true);

      const cityCodeDisabled = await streamRow.hasFieldStyle(
        testData.page,
        "public",
        "cities",
        "city_code",
        "disabled"
      );
      expect(cityCodeDisabled).toBe(true);
    });

    test("should have 'added' style after changing state from disabled => enabled", async () => {
      // Enable cities stream (continuing from previous test - no reload)
      await streamRow.toggleStreamSync(testData.page, "public", "cities", true);

      // Should have "added" style
      const hasAddedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "added");
      expect(hasAddedStyle).toBe(true);

      // Fields should no longer have "disabled" style
      const cityDisabled = await streamRow.hasFieldStyle(testData.page, "public", "cities", "city", "disabled");
      expect(cityDisabled).toBe(false);

      const cityCodeDisabled = await streamRow.hasFieldStyle(
        testData.page,
        "public",
        "cities",
        "city_code",
        "disabled"
      );
      expect(cityCodeDisabled).toBe(false);

      // Save - added style should clear
      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

      const hasAddedStyleAfterSave = await streamRow.hasStreamStyle(testData.page, "public", "cities", "added");
      expect(hasAddedStyleAfterSave).toBe(false);
    });

    test("should have 'changed' style after changing the sync mode", async () => {
      // Reload page
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Verify cities is enabled
      const isEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cities");
      expect(isEnabled).toBe(true);

      // Change sync mode to incremental/append_dedup
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.incremental,
        DestinationSyncMode.append_dedup
      );

      // Should have "changed" style
      const hasChangedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyle).toBe(true);

      // Select PK and cursor to make it valid
      await streamRow.selectPKs(testData.page, "public", "cities", ["city_code"]);
      await streamRow.selectCursor(testData.page, "public", "cities", "city");

      // Save with modal handling (incremental + append_dedup triggers modal)
      await replicationForm.saveChanges(testData.page, true);

      // Changed style should clear after save
      const hasChangedStyleAfterSave = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyleAfterSave).toBe(false);
    });

    test("should have 'changed' style after changing the PK", async () => {
      // Change PK (continuing from previous test - no reload)
      await streamRow.selectPKs(testData.page, "public", "cities", ["city"]);

      // Should have "changed" style
      const hasChangedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyle).toBe(true);

      // Discard changes - changed style should clear
      await streamsTable.clickDiscardChangesButton(testData.page);

      const hasChangedStyleAfterDiscard = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyleAfterDiscard).toBe(false);
    });

    test("should have 'changed' style after changing the cursor", async () => {
      // Change cursor (continuing from previous test - no reload)
      await streamRow.selectCursor(testData.page, "public", "cities", "city_code");

      // Should have "changed" style
      const hasChangedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyle).toBe(true);

      // Discard changes - changed style should clear
      await streamsTable.clickDiscardChangesButton(testData.page);

      const hasChangedStyleAfterDiscard = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(hasChangedStyleAfterDiscard).toBe(false);
    });
  });

  test.describe("Field styles", () => {
    test("should prepare fields for tests", async () => {
      // Change to full_refresh/overwrite so we can test field selection without PK/cursor constraints
      await streamRow.selectSyncMode(
        testData.page,
        "public",
        "cities",
        SyncMode.full_refresh,
        DestinationSyncMode.overwrite
      );

      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    test("should have field with 'removed' and stream with 'changed' styles after disabling the field", async () => {
      // Reload page
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Expand stream to access fields
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");

      // Disable city field
      await streamRow.toggleFieldSync(testData.page, "public", "cities", "city", false);

      // Stream should have "changed" style
      const streamHasChangedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(streamHasChangedStyle).toBe(true);

      // Field should have "removed" style
      const fieldHasRemovedStyle = await streamRow.hasFieldStyle(testData.page, "public", "cities", "city", "removed");
      expect(fieldHasRemovedStyle).toBe(true);

      // Save - styles should clear
      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

      const streamHasChangedStyleAfterSave = await streamRow.hasStreamStyle(
        testData.page,
        "public",
        "cities",
        "changed"
      );
      expect(streamHasChangedStyleAfterSave).toBe(false);

      const fieldHasRemovedStyleAfterSave = await streamRow.hasFieldStyle(
        testData.page,
        "public",
        "cities",
        "city",
        "removed"
      );
      expect(fieldHasRemovedStyleAfterSave).toBe(false);
    });

    test("should have field with 'added' and stream with 'changed' styles after enabling the field", async () => {
      // Reload page
      await navigateToReplicationPage(testData.page, workspaceId, testData.connection.connectionId);

      // Expand stream to access fields
      await streamRow.toggleExpandCollapse(testData.page, "public", "cities");

      // Enable city field
      await streamRow.toggleFieldSync(testData.page, "public", "cities", "city", true);

      // Stream should have "changed" style
      const streamHasChangedStyle = await streamRow.hasStreamStyle(testData.page, "public", "cities", "changed");
      expect(streamHasChangedStyle).toBe(true);

      // Field should have "added" style
      const fieldHasAddedStyle = await streamRow.hasFieldStyle(testData.page, "public", "cities", "city", "added");
      expect(fieldHasAddedStyle).toBe(true);

      // Save - styles should clear
      await streamsTable.clickSaveChangesButton(testData.page);
      await expect(testData.page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

      const streamHasChangedStyleAfterSave = await streamRow.hasStreamStyle(
        testData.page,
        "public",
        "cities",
        "changed"
      );
      expect(streamHasChangedStyleAfterSave).toBe(false);

      // Verify that the "added" style is cleared after save
      const fieldHasAddedStyleAfterSave = await streamRow.hasFieldStyle(
        testData.page,
        "public",
        "cities",
        "city",
        "added"
      );
      expect(fieldHasAddedStyleAfterSave).toBe(false);
    });
  });
});

/**
 * Deleted Connection Tests
 *
 * Tests that verify the UI correctly disables all edit controls when viewing
 * a connection that has been deleted. All interactive elements should be
 * disabled/read-only except for viewing and filtering capabilities.
 */
test.describe.serial("Sync Catalog - Deleted Connection", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Create Postgres → Postgres connection
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
      useMockSchemaDiscovery: true,
    });

    // Configure the users stream via API (incremental/append_dedup with PK and cursor)
    const connectionId = baseTestData.connection.connectionId;
    const streamToUpdate = baseTestData.connection.syncCatalog.streams.findIndex(
      (stream) => stream.stream?.name === "users" && stream.stream?.namespace === "public"
    );

    const updatedCatalog = {
      streams: [...baseTestData.connection.syncCatalog.streams],
    };

    updatedCatalog.streams[streamToUpdate].config = {
      ...updatedCatalog.streams[streamToUpdate].config,
      syncMode: SyncMode.incremental,
      destinationSyncMode: DestinationSyncMode.append_dedup,
      primaryKey: [["id"]],
      cursorField: ["email"],
      selected: true,
    };

    // Update connection with configured stream
    await connectionAPI.update(request, connectionId, {
      syncCatalog: updatedCatalog,
    });

    // Delete the connection via API
    await connectionAPI.delete(request, connectionId);

    // Navigate to the deleted connection's replication page
    await navigateToReplicationPage(page, workspaceId, connectionId);

    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    // Only cleanup source and destination (connection already deleted)
    if (testData.sourceId) {
      await postgresSourceAPI.delete(testData.request, testData.sourceId);
    }
    if (testData.destinationId) {
      await postgresDestinationAPI.delete(testData.request, testData.destinationId);
    }
    await testData.page.close();
    await testData.context.close();
  });

  test("should have stream filters still enabled", async () => {
    // Verify filter input is enabled
    const isFilterEnabled = await streamsTable.isFilterInputEnabled(testData.page);
    expect(isFilterEnabled).toBe(true);

    // Filter by stream name
    await streamsTable.filterByStreamName(testData.page, "users");
    const usersVisible = await streamRow.isStreamVisible(testData.page, "public", "users");
    expect(usersVisible).toBe(true);

    // Filter with no matches
    await streamsTable.filterByStreamName(testData.page, "userss");
    const hasNoMatchMsg = await streamsTable.hasNoMatchingStreamsMessage(testData.page);
    expect(hasNoMatchMsg).toBe(true);

    // Clear filter
    await streamsTable.clearFilterByStreamName(testData.page);

    // Verify all filter tabs are enabled
    const allTabEnabled = await streamsTable.isFilterTabEnabled(testData.page, "all");
    expect(allTabEnabled).toBe(true);

    const enabledTabEnabled = await streamsTable.isFilterTabEnabled(testData.page, "enabledStreams");
    expect(enabledTabEnabled).toBe(true);

    const disabledTabEnabled = await streamsTable.isFilterTabEnabled(testData.page, "disabledStreams");
    expect(disabledTabEnabled).toBe(true);

    // Switch to enabled streams tab
    await streamsTable.clickFilterTab(testData.page, "enabledStreams");
    const usersVisibleAfterFilter = await streamRow.isStreamVisible(testData.page, "public", "users");
    expect(usersVisibleAfterFilter).toBe(true);

    const usersEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "users");
    expect(usersEnabled).toBe(true);

    // Switch to disabled streams tab (should show "no streams")
    await streamsTable.clickFilterTab(testData.page, "disabledStreams");
    const hasNoStreamsMsg = await streamsTable.hasNoStreamsMessage(testData.page);
    expect(hasNoStreamsMsg).toBe(true);

    // Switch back to all
    await streamsTable.clickFilterTab(testData.page, "all");
  });

  test("should not allow refreshing the source schema", async () => {
    const refreshButtonExists = await streamsTable.refreshSchemaButtonExists(testData.page);
    expect(refreshButtonExists).toBe(true);

    const refreshButtonEnabled = await streamsTable.isRefreshSchemaButtonEnabled(testData.page);
    expect(refreshButtonEnabled).toBe(false);
  });

  test("should allow expanding and collapsing all streams", async () => {
    const expandAllButtonExists = await streamsTable.expandCollapseAllStreamsButtonExists(testData.page);
    expect(expandAllButtonExists).toBe(true);

    const expandAllButtonEnabled = await streamsTable.isExpandCollapseAllStreamsButtonEnabled(testData.page);
    expect(expandAllButtonEnabled).toBe(true);
  });

  test("should not allow enabling or disabling all streams in a namespace", async () => {
    const namespaceCheckboxEnabled = await streamsTable.isNamespaceCheckboxEnabled(testData.page);
    expect(namespaceCheckboxEnabled).toBe(false);
  });

  test("should not allow enabling or disabling individual streams", async () => {
    const streamCheckboxDisabled = await streamRow.isStreamSyncCheckboxDisabled(testData.page, "public", "users");
    expect(streamCheckboxDisabled).toBe(true);
  });

  test("should not allow enabling or disabling individual fields in a stream", async () => {
    // Expand stream to access fields
    await streamRow.toggleExpandCollapse(testData.page, "public", "users");

    const fieldCheckboxDisabled = await streamRow.isFieldSyncCheckboxDisabled(testData.page, "public", "users", "id");
    expect(fieldCheckboxDisabled).toBe(true);
  });

  test("should allow expanding and collapsing a single stream", async () => {
    const expandButtonEnabled = await streamRow.isExpandButtonEnabled(testData.page, "public", "users");
    expect(expandButtonEnabled).toBe(true);
  });

  test("should not allow changing the stream sync mode", async () => {
    const syncModeDisabled = await streamRow.isSyncModeDropdownDisabled(testData.page, "public", "users");
    expect(syncModeDisabled).toBe(true);
  });

  test("should not allow changing the selected Primary Key and Cursor", async () => {
    const pkDisabled = await streamRow.isPKComboboxDisabled(testData.page, "public", "users");
    expect(pkDisabled).toBe(true);

    const cursorDisabled = await streamRow.isCursorComboboxDisabled(testData.page, "public", "users");
    expect(cursorDisabled).toBe(true);
  });
});

/**
 * Tab Filter Tests
 *
 * Tests the tab filtering functionality for streams:
 * - "All" tab: Shows all streams regardless of enabled state
 * - "Enabled streams" tab: Shows only enabled streams
 * - "Disabled streams" tab: Shows only disabled streams
 *
 * Also tests:
 * - Stream name filtering within each tab
 * - Namespace row stream counts (e.g., "3 streams", "1 / 3 streams")
 * - Empty state messages when no streams match filters
 *
 * Uses 3 streams: users, cities, cars (all disabled initially via enableAllStreams: false)
 */
test.describe.serial("Sync Catalog - Tab Filters", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext();
    const page = await context.newPage();
    const request = context.request;

    // Create Postgres → Postgres connection (explicitly disable all streams to start)
    // NOTE: This test suite toggles stream states and expects them to persist across tests.
    // Mocking is intentionally disabled here as it doesn't preserve state correctly through updates.
    const baseTestData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
      enableAllStreams: false,
    });

    // Navigate to replication page
    await navigateToReplicationPage(page, workspaceId, baseTestData.connection.connectionId);

    testData = { ...baseTestData, page, context, request };
  });

  test.afterAll(async () => {
    await connectionTestScaffold.cleanupConnection(testData.request, testData);
    await testData.page.close();
    await testData.context.close();
  });

  test.describe("All tab", () => {
    test("should show all streams (enabled and disabled)", async () => {
      await streamsTable.clickFilterTab(testData.page, "all");

      // All streams should be visible regardless of enabled state
      const usersVisible = await streamRow.isStreamVisible(testData.page, "public", "users");
      expect(usersVisible).toBe(true);

      const citiesVisible = await streamRow.isStreamVisible(testData.page, "public", "cities");
      expect(citiesVisible).toBe(true);

      const carsVisible = await streamRow.isStreamVisible(testData.page, "public", "cars");
      expect(carsVisible).toBe(true);

      // Enable cars for subsequent enabled/disabled tab tests
      await streamRow.toggleStreamSync(testData.page, "public", "cars", true);
      const carsStillVisible = await streamRow.isStreamVisible(testData.page, "public", "cars");
      expect(carsStillVisible).toBe(true);
    });

    test("should show total count of streams in namespace row", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(testData.page);
      expect(countText).toContain("3");
      expect(countText).toContain("stream");
    });
  });

  test.describe("Enabled streams tab", () => {
    test("should show only enabled streams", async () => {
      await streamsTable.clickFilterTab(testData.page, "enabledStreams");

      // Only cars should be visible (enabled in previous test)
      const usersVisible = await streamRow.isStreamVisible(testData.page, "public", "users");
      expect(usersVisible).toBe(false);

      const citiesVisible = await streamRow.isStreamVisible(testData.page, "public", "cities");
      expect(citiesVisible).toBe(false);

      const carsVisible = await streamRow.isStreamVisible(testData.page, "public", "cars");
      expect(carsVisible).toBe(true);

      const carsEnabled = await streamRow.isStreamSyncEnabled(testData.page, "public", "cars");
      expect(carsEnabled).toBe(true);
    });

    test("should show only enabled streams filtered by name", async () => {
      await streamsTable.filterByStreamName(testData.page, "cars");
      const carsVisible = await streamRow.isStreamVisible(testData.page, "public", "cars");
      expect(carsVisible).toBe(true);

      // Test no matches scenario
      await streamsTable.filterByStreamName(testData.page, "carss");
      const hasNoMatchMsg = await streamsTable.hasNoMatchingStreamsMessage(testData.page);
      expect(hasNoMatchMsg).toBe(true);

      await streamsTable.clearFilterByStreamName(testData.page);
    });

    test("should show `{enabled} / {total} streams` count in namespace row if not all streams are enabled", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(testData.page);
      expect(countText).toContain("1");
      expect(countText).toContain("3");
    });

    test("should show empty table if there is no enabled streams", async () => {
      // Disable cars to test empty state
      await streamRow.toggleStreamSync(testData.page, "public", "cars", false);

      const isNamespaceCellEmpty = await streamsTable.isNamespaceCellEmpty(testData.page);
      expect(isNamespaceCellEmpty).toBe(true);

      const hasNoStreamsMsg = await streamsTable.hasNoStreamsMessage(testData.page);
      expect(hasNoStreamsMsg).toBe(true);
    });
  });

  test.describe("Disabled streams tab", () => {
    test("should show only disabled streams", async () => {
      await streamsTable.clickFilterTab(testData.page, "disabledStreams");

      // All streams should be visible (all disabled after previous test)
      const usersVisible = await streamRow.isStreamVisible(testData.page, "public", "users");
      expect(usersVisible).toBe(true);

      const citiesVisible = await streamRow.isStreamVisible(testData.page, "public", "cities");
      expect(citiesVisible).toBe(true);

      const carsVisible = await streamRow.isStreamVisible(testData.page, "public", "cars");
      expect(carsVisible).toBe(true);
    });

    test("should show only disabled streams filtered by name", async () => {
      // Enable cities - it should disappear from disabled tab
      await streamRow.toggleStreamSync(testData.page, "public", "cities", true);
      const citiesVisible = await streamRow.isStreamVisible(testData.page, "public", "cities");
      expect(citiesVisible).toBe(false);

      // Filter by "cities" should show no matches (cities is now enabled)
      await streamsTable.filterByStreamName(testData.page, "cities");
      const isNamespaceCellEmpty = await streamsTable.isNamespaceCellEmpty(testData.page);
      expect(isNamespaceCellEmpty).toBe(true);

      const hasNoMatchMsg = await streamsTable.hasNoMatchingStreamsMessage(testData.page);
      expect(hasNoMatchMsg).toBe(true);

      await streamsTable.clearFilterByStreamName(testData.page);
    });

    test("should show `{disabled} / {total} streams` count in namespace row if not all streams are disabled", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(testData.page);
      expect(countText).toContain("2");
      expect(countText).toContain("3");
    });
  });
});
