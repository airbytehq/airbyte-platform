/**
 * Sync Catalog Tests
 *
 * Tests the sync catalog UI for configuring stream replication, including:
 * - Stream and field selection
 */

import type { CatalogOptions, MockAirbyte } from "../../mock";

import { test, expect, Page, BrowserContext, APIRequestContext } from "@playwright/test";
import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

import { connectionTestScaffold, ConnectionTestData, connectionAPI } from "../../helpers/connection";
import { postgresSourceAPI, postgresDestinationAPI } from "../../helpers/connectors";
import { streamsTable, replicationForm, navigateToReplicationPage } from "../../helpers/replication";
import { streamRow } from "../../helpers/streamRow";
import { setupWorkspaceForTests } from "../../helpers/workspace";
// `mockTest` runs against the new mock backend. `test` uses the old helpers. Tests in this file move to `mockTest` one at a time.
import { catalogs, connection, defs, destination, scenario, source, test as mockTest } from "../../mock";
import { setFeatureFlags, setFeatureServiceFlags, injectFeatureFlagsAndStyle } from "../../support/e2e";

// Extended test data type that includes shared browser resources for serial tests
interface SerialTestData extends ConnectionTestData {
  page: Page;
  context: BrowserContext;
  request: APIRequestContext;
}

// Workspace of the deployed backend. Each live describe sets it in its beforeAll, so the mocked tests in this
// file do not call the deployed backend.
let workspaceId: string;

test.describe.serial("Sync Catalog - Stream state tests", () => {
  let testData: SerialTestData;

  test.beforeAll(async ({ browser }) => {
    workspaceId = await setupWorkspaceForTests();
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
    workspaceId = await setupWorkspaceForTests();
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
 * Sync Modes Tests - mocked API
 *
 * Tests the combinations of sync modes (Full refresh, Incremental) with destination modes (Append, Overwrite,
 * Overwrite + Deduped, Append + Deduped). Each test:
 * 1. Configures a stream with the sync mode
 * 2. Checks the primary key (PK) and cursor controls and their errors
 * 3. Saves the configuration
 * 4. Reloads the page and checks the saved configuration
 *
 * Uses 2 streams: users (with a source-defined PK) and cities (without a source-defined PK).
 *
 * The tests are independent. Each test sets its own start state in its scenario. Most tests start from the
 * default scenario: every stream is selected as Full refresh | Overwrite, with no PKs that the user selected.
 */
const postgresConnection = connection({
  source: source({ def: defs.postgresSource, name: "Postgres source" }),
  destination: destination({ def: defs.postgresDestination, name: "Postgres destination" }),
  catalog: catalogs.postgres({ selected: true }),
});

/** A scenario with the Postgres connection. Every stream is selected. `streams` changes the config of some streams. */
function syncModesScenario(streams: CatalogOptions["streams"] = {}) {
  return scenario({
    connections: [postgresConnection.with({ catalog: catalogs.postgres({ selected: true, streams }) })],
  });
}

/** cities as Full refresh | Overwrite + Deduped, with two PKs that the user selected. */
const citiesWithTwoPKs = {
  destinationSyncMode: DestinationSyncMode.overwrite_dedup,
  primaryKey: [["city_code"], ["city"]],
};

/** The config of one stream in the last connection update that the webapp sent. */
function lastSavedStreamConfig(airbyte: MockAirbyte, streamName: string) {
  return catalogs.find(airbyte.calls.last("webBackendUpdateConnection")?.syncCatalog, "public", streamName).config;
}

mockTest.describe("Sync Catalog - Sync Modes - mocked API", () => {
  mockTest.describe(() => {
    mockTest.use({ airbyteScenario: syncModesScenario() });

    mockTest("Full refresh | Append", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.full_refresh, DestinationSyncMode.append);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.append
        );
      });

      await mockTest.step("should not display the PK and Cursor combobox buttons", async () => {
        expect(await streamRow.isPKComboboxDisplayed(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect(await streamRow.isCursorComboboxDisplayed(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
      });

      await mockTest.step("should allow to save changes", async () => {
        expect(await streamsTable.hasNoStreamsSelectedError(page)).toBe(false);
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        const updates = airbyte.calls("webBackendUpdateConnection");
        expect(updates).toHaveLength(1);
        expect(updates[0]).toMatchObject({
          connectionId: initial.connectionId,
          sourceCatalogId: initial.catalogId,
          skipReset: true,
        });
        expect(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        });
        // Streams the test did not touch are sent unchanged.
        expect(lastSavedStreamConfig(airbyte, "cars")).toMatchObject({
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
        });
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.append
        );
        expect(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: initial.connectionId,
          withRefreshedCatalog: false,
        });
      });
    });
  });

  mockTest.describe(() => {
    // users starts as Full refresh | Append, so selecting Overwrite changes the form.
    mockTest.use({
      airbyteScenario: syncModesScenario({ users: { destinationSyncMode: DestinationSyncMode.append } }),
    });

    mockTest("Full refresh | Overwrite", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.full_refresh, DestinationSyncMode.overwrite);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite
        );
      });

      await mockTest.step("should not display the PK and Cursor combobox buttons", async () => {
        expect(await streamRow.isPKComboboxDisplayed(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect(await streamRow.isCursorComboboxDisplayed(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
      });

      await mockTest.step("should allow to save changes", async () => {
        expect(await streamsTable.hasNoStreamsSelectedError(page)).toBe(false);
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
        });
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite
        );
      });
    });
  });

  mockTest.describe(() => {
    mockTest.use({ airbyteScenario: syncModesScenario() });

    mockTest("Full refresh | Overwrite + Deduped", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "cities", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);

        await streamRow.selectSyncMode(
          page,
          "public",
          "cities",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "cities",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
      });

      await mockTest.step("should show missing PK error", async () => {
        expect(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(true);
        expect(await streamRow.hasMissingCursorError(page, "public", "cities")).toBe(false);
        expect(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await mockTest.step("should select PK", async () => {
        await streamRow.selectPKs(page, "public", "cities", ["city_code"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "city_code");
        expect(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(false);

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
      });

      await mockTest.step("should allow to save changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect(lastSavedStreamConfig(airbyte, "cities")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite_dedup,
          primaryKey: [["city_code"]],
        });
      });

      await mockTest.step("should select multiple PKs", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        // city_code is already a PK from the save above.
        await streamRow.selectPKs(page, "public", "cities", ["city"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "2 items selected");

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
        expect(await streamRow.isFieldPK(page, "public", "cities", "city")).toBe(true);

        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(2);
        expect(lastSavedStreamConfig(airbyte, "cities")).toMatchObject(citiesWithTwoPKs);
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);
        await streamRow.verifySelectedPK(page, "public", "cities", "2 items selected");
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "cities",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
      });
    });
  });

  mockTest.describe(() => {
    mockTest.use({ airbyteScenario: syncModesScenario() });

    mockTest("Full refresh | Overwrite + Deduped (source-defined PK)", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
      });

      await mockTest.step("should NOT show missing PK and Cursor error", async () => {
        expect(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
        expect(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(true);
      });

      await mockTest.step("should show non-editable selected PK", async () => {
        await streamRow.verifySelectedPK(page, "public", "users", "id");
        expect(await streamRow.isPKComboboxDisabled(page, "public", "users")).toBe(true);

        await streamRow.toggleExpandCollapse(page, "public", "users");
        expect(await streamRow.isFieldPK(page, "public", "users", "id")).toBe(true);
      });

      await mockTest.step("should allow to save changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite_dedup,
          primaryKey: [["id"]],
        });
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
        await streamRow.verifySelectedPK(page, "public", "users", "id");
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite_dedup
        );
      });
    });
  });

  mockTest.describe(() => {
    // users starts as Full refresh | Append. From Full refresh | Overwrite, a change to Incremental opens a refresh
    // modal on save.
    mockTest.use({
      airbyteScenario: syncModesScenario({ users: { destinationSyncMode: DestinationSyncMode.append } }),
    });

    mockTest("Incremental | Append", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.incremental, DestinationSyncMode.append);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.incremental,
          DestinationSyncMode.append
        );
      });

      await mockTest.step("should show missing Cursor error", async () => {
        expect(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(true);
        expect(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await mockTest.step("should select Cursor", async () => {
        await streamRow.selectCursor(page, "public", "users", "email");
        await streamRow.verifySelectedCursor(page, "public", "users", "email");

        await streamRow.toggleExpandCollapse(page, "public", "users");
        expect(await streamRow.isFieldCursor(page, "public", "users", "email")).toBe(true);
      });

      await mockTest.step("should allow to save changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: ["email"],
        });
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
        await streamRow.verifySelectedCursor(page, "public", "users", "email");
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.incremental,
          DestinationSyncMode.append
        );
      });
    });
  });

  mockTest.describe(() => {
    mockTest.use({ airbyteScenario: syncModesScenario() });

    mockTest("Incremental | Append + Deduped", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await mockTest.step("should select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "cities", true);
        expect(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);

        await streamRow.selectSyncMode(
          page,
          "public",
          "cities",
          SyncMode.incremental,
          DestinationSyncMode.append_dedup
        );
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "cities",
          SyncMode.incremental,
          DestinationSyncMode.append_dedup
        );
      });

      await mockTest.step("should show missing PK and Cursor errors", async () => {
        expect(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(true);
        expect(await streamRow.hasMissingCursorError(page, "public", "cities")).toBe(true);
        expect(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await mockTest.step("should select PK and Cursor", async () => {
        await streamRow.selectPKs(page, "public", "cities", ["city_code"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "city_code");

        await streamRow.selectCursor(page, "public", "cities", "city");
        await streamRow.verifySelectedCursor(page, "public", "cities", "city");

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
        expect(await streamRow.isFieldCursor(page, "public", "cities", "city")).toBe(true);
      });

      await mockTest.step("should allow to save changes and discard refresh streams", async () => {
        // The change to Incremental | Append + Deduped opens a modal that offers a refresh. Save without one.
        await replicationForm.saveChanges(page, true);

        expect(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({ skipReset: true });
        expect(lastSavedStreamConfig(airbyte, "cities")).toMatchObject({
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [["city_code"]],
          cursorField: ["city"],
        });
      });

      await mockTest.step("should verify that changes are applied", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);
        await streamRow.verifySelectedPK(page, "public", "cities", "city_code");
        await streamRow.verifySelectedCursor(page, "public", "cities", "city");
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "cities",
          SyncMode.incremental,
          DestinationSyncMode.append_dedup
        );
      });
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
    workspaceId = await setupWorkspaceForTests();
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
    workspaceId = await setupWorkspaceForTests();
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
    workspaceId = await setupWorkspaceForTests();
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
