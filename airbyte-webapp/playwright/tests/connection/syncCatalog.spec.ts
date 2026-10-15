/**
 * Sync Catalog Tests
 *
 * Tests the sync catalog UI for configuring stream replication, including:
 * - Stream and field selection
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it. The check for the "saved"
 * notification stays a normal `expect`, because the next step needs the save to be finished.
 */

import type { CatalogOptions, MockAirbyte } from "../../mock";
import type { Page } from "@playwright/test";

import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

import { streamsTable, replicationForm, navigateToReplicationPage } from "../../helpers/replication";
import { streamRow } from "../../helpers/streamRow";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

/**
 * The Postgres → Postgres connection that every test uses. Its catalog has the streams cars, cities and users in the
 * namespace "public" (createMockPostgresCatalog in helpers/mocks.ts).
 */
const postgresConnection = connection({
  source: source({ def: defs.postgresSource, name: "Postgres source" }),
  destination: destination({ def: defs.postgresDestination, name: "Postgres destination" }),
  catalog: catalogs.postgres({ selected: true }),
});

/** A scenario with the Postgres connection. `catalog` sets which streams are selected and the config of each stream. */
function postgresScenario(catalog: CatalogOptions) {
  return scenario({ connections: [postgresConnection.with({ catalog: catalogs.postgres(catalog) })] });
}

/** Opens the replication page of the Postgres connection and waits for the streams table. */
async function openReplicationPage(page: Page, airbyte: MockAirbyte) {
  const { connectionId } = airbyte.store.connections.get(postgresConnection);
  await navigateToReplicationPage(page, airbyte.workspaceId, connectionId);
}

/**
 * Stream State Tests
 *
 * Tests the stream checkbox, the field checkboxes, and the sync mode, PK and cursor controls of the cars stream when
 * the stream is enabled and when it is disabled. cars has the fields color, id, mark and model, and the
 * source-defined PK id.
 */
test.describe("Stream state", () => {
  test.describe(() => {
    // Every stream is selected as Full refresh | Overwrite.
    test.use({ airbyteScenario: postgresScenario({ selected: true }) });

    test("configure the sync mode of an enabled stream, then disable the stream", async ({ page, airbyte }) => {
      await openReplicationPage(page, airbyte);

      await test.step("the stream is enabled", async () => {
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(true);
      });

      await test.step("every field of the stream is enabled", async () => {
        await streamRow.toggleExpandCollapse(page, "public", "cars");

        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "color")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "mark")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "model")).toBe(true);
      });

      await test.step("select Incremental | Append + Deduped", async () => {
        await streamRow.selectSyncMode(page, "public", "cars", SyncMode.incremental, DestinationSyncMode.append_dedup);

        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "cars",
          SyncMode.incremental,
          DestinationSyncMode.append_dedup
        );
      });

      await test.step("the source-defined PK is selected", async () => {
        // Append + Deduped needs a PK. The form uses the source-defined PK id.
        await streamRow.verifySelectedPK(page, "public", "cars", "id");
      });

      await test.step("the missing cursor error shows", async () => {
        // Incremental needs a cursor, and cars has no cursor yet.
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "cars")).toBe(true);
      });

      await test.step("disable the stream", async () => {
        await streamRow.toggleStreamSync(page, "public", "cars", false);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(false);
      });

      await test.step("every field of the stream is disabled", async () => {
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "color")).toBe(false);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "id")).toBe(false);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "mark")).toBe(false);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "model")).toBe(false);
      });

      await test.step("the sync mode dropdown is hidden", async () => {
        expect.soft(await streamRow.isSyncModeDropdownDisplayed(page, "public", "cars")).toBe(false);
      });

      await test.step("the PK combobox is hidden", async () => {
        expect.soft(await streamRow.isPKComboboxDisplayed(page, "public", "cars")).toBe(false);
      });

      await test.step("the cursor combobox is hidden", async () => {
        expect.soft(await streamRow.isCursorComboboxDisplayed(page, "public", "cars")).toBe(false);
      });
    });
  });

  test.describe(() => {
    // Every stream is selected except cars. cars is Full refresh | Overwrite.
    test.use({ airbyteScenario: postgresScenario({ selected: true, streams: { cars: { selected: false } } }) });

    test("enable and disable the fields of a stream", async ({ page, airbyte }) => {
      await openReplicationPage(page, airbyte);

      await test.step("the field checkboxes show whether or not the stream is enabled", async () => {
        await streamRow.toggleExpandCollapse(page, "public", "cars");

        await streamRow.toggleStreamSync(page, "public", "cars", true);
        expect.soft(await streamRow.isFieldSyncCheckboxDisplayed(page, "public", "cars", "color")).toBe(true);

        await streamRow.toggleStreamSync(page, "public", "cars", false);
        expect.soft(await streamRow.isFieldSyncCheckboxDisplayed(page, "public", "cars", "color")).toBe(true);

        await streamRow.toggleStreamSync(page, "public", "cars", true);
      });

      await test.step("each field shows its type", async () => {
        await streamRow.verifyFieldType(page, "public", "cars", "id", "Integer");
        await streamRow.verifyFieldType(page, "public", "cars", "color", "String");
      });

      await test.step("the PK field and the cursor field cannot be disabled", async () => {
        await streamRow.selectSyncMode(page, "public", "cars", SyncMode.incremental, DestinationSyncMode.append_dedup);
        await streamRow.selectCursor(page, "public", "cars", "mark");

        // A PK field and a cursor field must sync, so their checkboxes are checked and the user cannot change them.
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldSyncCheckboxDisabled(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "mark")).toBe(true);
        expect.soft(await streamRow.isFieldSyncCheckboxDisabled(page, "public", "cars", "mark")).toBe(true);
      });

      await test.step("the PK field and the cursor field have labels", async () => {
        expect.soft(await streamRow.isFieldPK(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldCursor(page, "public", "cars", "mark")).toBe(true);
      });

      await test.step("enabling one field enables the stream and its required fields", async () => {
        await streamRow.toggleStreamSync(page, "public", "cars", false);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(false);

        // Selecting one field enables the stream and its required fields: the PK id and the cursor mark.
        await streamRow.toggleFieldSync(page, "public", "cars", "color", true);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "color")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldSyncCheckboxDisabled(page, "public", "cars", "id")).toBe(true);
        expect.soft(await streamRow.isFieldSyncEnabled(page, "public", "cars", "mark")).toBe(true);
        expect.soft(await streamRow.isFieldSyncCheckboxDisabled(page, "public", "cars", "mark")).toBe(true);
      });

      await test.step("disabling every field disables the stream", async () => {
        // Full refresh | Append needs no PK or cursor, so the user can unselect every field. Only color, id and mark
        // are selected now, so unselecting these three leaves no field selected.
        await streamRow.selectSyncMode(page, "public", "cars", SyncMode.full_refresh, DestinationSyncMode.append);
        await streamRow.toggleFieldSync(page, "public", "cars", "color", false);
        await streamRow.toggleFieldSync(page, "public", "cars", "id", false);
        await streamRow.toggleFieldSync(page, "public", "cars", "mark", false);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(false);
      });
    });
  });
});

/**
 * Feature Flag Tests
 *
 * Tests the sync catalog when the experiment "connection.columnSelection" is off.
 */
test.describe(() => {
  // Every stream is selected as Full refresh | Overwrite. The experiment turns off field selection.
  test.use({
    airbyteScenario: postgresScenario({ selected: true }).with({
      experiments: { "connection.columnSelection": false },
    }),
  });

  test("configure a stream when the column selection experiment is off", async ({ page, airbyte }) => {
    await openReplicationPage(page, airbyte);

    await test.step("enable cars as Incremental | Append + Deduped with the cursor mark, and expand it", async () => {
      await streamRow.toggleStreamSync(page, "public", "cars", true);
      await streamRow.selectSyncMode(page, "public", "cars", SyncMode.incremental, DestinationSyncMode.append_dedup);
      await streamRow.selectCursor(page, "public", "cars", "mark");
      await streamRow.toggleExpandCollapse(page, "public", "cars");
    });

    await test.step("the field checkboxes are hidden", async () => {
      // isFieldSyncCheckboxDisplayed also returns false when the field rows do not render. This check makes sure
      // that the field rows render, so the checks after it test the experiment.
      await streamRow.verifyFieldType(page, "public", "cars", "id", "Integer");

      expect.soft(await streamRow.isFieldSyncCheckboxDisplayed(page, "public", "cars", "id")).toBe(false);
      expect.soft(await streamRow.isFieldSyncCheckboxDisplayed(page, "public", "cars", "mark")).toBe(false);
      expect.soft(await streamRow.isFieldSyncCheckboxDisplayed(page, "public", "cars", "color")).toBe(false);
    });
  });
});

/**
 * Sync Modes Tests
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

test.describe("Sync modes", () => {
  test.describe(() => {
    test.use({ airbyteScenario: syncModesScenario() });

    test("save a stream as Full refresh | Append", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.full_refresh, DestinationSyncMode.append);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.append
        );
      });

      await test.step("the PK and cursor comboboxes are hidden", async () => {
        expect.soft(await streamRow.isPKComboboxDisplayed(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.isCursorComboboxDisplayed(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
      });

      await test.step("save the changes", async () => {
        expect.soft(await streamsTable.hasNoStreamsSelectedError(page)).toBe(false);
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        const updates = airbyte.calls("webBackendUpdateConnection");
        expect.soft(updates).toHaveLength(1);
        expect.soft(updates[0]).toMatchObject({
          connectionId: initial.connectionId,
          sourceCatalogId: initial.catalogId,
          skipReset: true,
        });
        expect.soft(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        });
        // Streams the test did not touch are sent unchanged.
        expect.soft(lastSavedStreamConfig(airbyte, "cars")).toMatchObject({
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
        });
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.append
        );
        expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: initial.connectionId,
          withRefreshedCatalog: false,
        });
      });
    });
  });

  test.describe(() => {
    // users starts as Full refresh | Append, so selecting Overwrite changes the form.
    test.use({
      airbyteScenario: syncModesScenario({ users: { destinationSyncMode: DestinationSyncMode.append } }),
    });

    test("save a stream as Full refresh | Overwrite", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.full_refresh, DestinationSyncMode.overwrite);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.full_refresh,
          DestinationSyncMode.overwrite
        );
      });

      await test.step("the PK and cursor comboboxes are hidden", async () => {
        expect.soft(await streamRow.isPKComboboxDisplayed(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.isCursorComboboxDisplayed(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
      });

      await test.step("save the changes", async () => {
        expect.soft(await streamsTable.hasNoStreamsSelectedError(page)).toBe(false);
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect.soft(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
        });
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
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

  test.describe(() => {
    test.use({ airbyteScenario: syncModesScenario() });

    test("save a stream as Full refresh | Overwrite + Deduped with one PK, then two PKs", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "cities", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);

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

      await test.step("the missing PK error shows", async () => {
        expect.soft(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(true);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "cities")).toBe(false);
        expect.soft(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await test.step("select a PK", async () => {
        await streamRow.selectPKs(page, "public", "cities", ["city_code"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "city_code");
        expect.soft(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(false);

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect.soft(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
      });

      await test.step("save the changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect.soft(lastSavedStreamConfig(airbyte, "cities")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite_dedup,
          primaryKey: [["city_code"]],
        });
      });

      await test.step("select a second PK and save", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        // city_code is already a PK from the save above.
        await streamRow.selectPKs(page, "public", "cities", ["city"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "2 items selected");

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect.soft(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
        expect.soft(await streamRow.isFieldPK(page, "public", "cities", "city")).toBe(true);

        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(2);
        expect.soft(lastSavedStreamConfig(airbyte, "cities")).toMatchObject(citiesWithTwoPKs);
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);
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

  test.describe(() => {
    test.use({ airbyteScenario: syncModesScenario() });

    test("save a stream as Full refresh | Overwrite + Deduped with its source-defined PK", async ({
      page,
      airbyte,
    }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

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

      await test.step("no PK or cursor error shows", async () => {
        expect.soft(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(false);
        expect.soft(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(true);
      });

      await test.step("the source-defined PK is selected and cannot change", async () => {
        await streamRow.verifySelectedPK(page, "public", "users", "id");
        expect.soft(await streamRow.isPKComboboxDisabled(page, "public", "users")).toBe(true);

        await streamRow.toggleExpandCollapse(page, "public", "users");
        expect.soft(await streamRow.isFieldPK(page, "public", "users", "id")).toBe(true);
      });

      await test.step("save the changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect.soft(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite_dedup,
          primaryKey: [["id"]],
        });
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
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

  test.describe(() => {
    // users starts as Full refresh | Append. From Full refresh | Overwrite, a change to Incremental opens a refresh
    // modal on save.
    test.use({
      airbyteScenario: syncModesScenario({ users: { destinationSyncMode: DestinationSyncMode.append } }),
    });

    test("save a stream as Incremental | Append with a cursor", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "users", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

        await streamRow.selectSyncMode(page, "public", "users", SyncMode.incremental, DestinationSyncMode.append);
        await streamRow.verifySelectedSyncMode(
          page,
          "public",
          "users",
          SyncMode.incremental,
          DestinationSyncMode.append
        );
      });

      await test.step("the missing cursor error shows", async () => {
        expect.soft(await streamRow.hasMissingPKError(page, "public", "users")).toBe(false);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "users")).toBe(true);
        expect.soft(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await test.step("select a cursor", async () => {
        await streamRow.selectCursor(page, "public", "users", "email");
        await streamRow.verifySelectedCursor(page, "public", "users", "email");

        await streamRow.toggleExpandCollapse(page, "public", "users");
        expect.soft(await streamRow.isFieldCursor(page, "public", "users", "email")).toBe(true);
      });

      await test.step("save the changes", async () => {
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect.soft(lastSavedStreamConfig(airbyte, "users")).toMatchObject({
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: ["email"],
        });
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);
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

  test.describe(() => {
    test.use({ airbyteScenario: syncModesScenario() });

    test("save a stream as Incremental | Append + Deduped without a refresh", async ({ page, airbyte }) => {
      const initial = airbyte.store.connections.get(postgresConnection);

      await test.step("select the sync mode", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);
        await streamRow.toggleStreamSync(page, "public", "cities", true);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);

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

      await test.step("the missing PK and cursor errors show", async () => {
        expect.soft(await streamRow.hasMissingPKError(page, "public", "cities")).toBe(true);
        expect.soft(await streamRow.hasMissingCursorError(page, "public", "cities")).toBe(true);
        expect.soft(await streamsTable.isSaveChangesButtonEnabled(page)).toBe(false);
      });

      await test.step("select a PK and a cursor", async () => {
        await streamRow.selectPKs(page, "public", "cities", ["city_code"]);
        await streamRow.verifySelectedPK(page, "public", "cities", "city_code");

        await streamRow.selectCursor(page, "public", "cities", "city");
        await streamRow.verifySelectedCursor(page, "public", "cities", "city");

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect.soft(await streamRow.isFieldPK(page, "public", "cities", "city_code")).toBe(true);
        expect.soft(await streamRow.isFieldCursor(page, "public", "cities", "city")).toBe(true);
      });

      await test.step("save the changes without a refresh", async () => {
        // The change to Incremental | Append + Deduped opens a modal that offers a refresh. Save without one.
        await replicationForm.saveChanges(page, true);

        expect.soft(airbyte.calls.count("webBackendUpdateConnection")).toBe(1);
        expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({ skipReset: true });
        expect.soft(lastSavedStreamConfig(airbyte, "cities")).toMatchObject({
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [["city_code"]],
          cursorField: ["city"],
        });
      });

      await test.step("the saved configuration shows after a reload", async () => {
        await navigateToReplicationPage(page, airbyte.workspaceId, initial.connectionId);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);
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
 *
 * Each test is one user journey on the cities stream. Each step continues from the page and the saved state that
 * the step before it left.
 */
test.describe("Diff styles", () => {
  test.describe(() => {
    // Every stream is selected as Full refresh | Overwrite.
    test.use({ airbyteScenario: postgresScenario({ selected: true }) });

    test("see my unsaved changes to streams", async ({ page, airbyte }) => {
      await openReplicationPage(page, airbyte);

      await test.step("a disabled stream shows as removed until I save", async () => {
        await streamRow.toggleStreamSync(page, "public", "cities", false);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "removed")).toBe(true);

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "disabled")).toBe(true);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city_code", "disabled")).toBe(true);

        // Saving clears the "removed" style.
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "removed")).toBe(false);
      });

      await test.step("a saved disabled stream shows as disabled", async () => {
        await openReplicationPage(page, airbyte);

        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(false);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "disabled")).toBe(true);

        await streamRow.toggleExpandCollapse(page, "public", "cities");
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "disabled")).toBe(true);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city_code", "disabled")).toBe(true);
      });

      await test.step("an enabled stream shows as added until I save", async () => {
        await streamRow.toggleStreamSync(page, "public", "cities", true);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "added")).toBe(true);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "disabled")).toBe(false);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city_code", "disabled")).toBe(false);

        // Saving clears the "added" style.
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "added")).toBe(false);
      });

      await test.step("a sync mode change shows the stream as changed until I save", async () => {
        await openReplicationPage(page, airbyte);
        expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cities")).toBe(true);

        await streamRow.selectSyncMode(
          page,
          "public",
          "cities",
          SyncMode.incremental,
          DestinationSyncMode.append_dedup
        );
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(true);

        // Append + Deduped needs a PK and Incremental needs a cursor. Without them the form cannot be saved.
        await streamRow.selectPKs(page, "public", "cities", ["city_code"]);
        await streamRow.selectCursor(page, "public", "cities", "city");

        // The change to Incremental | Append + Deduped opens a modal that offers a refresh. Save without one.
        await replicationForm.saveChanges(page, true);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(false);
      });

      await test.step("a PK change shows the stream as changed until I discard it", async () => {
        await streamRow.selectPKs(page, "public", "cities", ["city"]);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(true);

        await streamsTable.clickDiscardChangesButton(page);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(false);
      });

      await test.step("a cursor change shows the stream as changed until I discard it", async () => {
        await streamRow.selectCursor(page, "public", "cities", "city_code");
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(true);

        await streamsTable.clickDiscardChangesButton(page);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(false);
      });
    });
  });

  test.describe(() => {
    // Every stream is selected. cities is Incremental | Append + Deduped with the PK city_code and the cursor city.
    test.use({
      airbyteScenario: postgresScenario({
        selected: true,
        streams: {
          cities: {
            syncMode: SyncMode.incremental,
            destinationSyncMode: DestinationSyncMode.append_dedup,
            primaryKey: [["city_code"]],
            cursorField: ["city"],
          },
        },
      }),
    });

    test("see my unsaved changes to fields", async ({ page, airbyte }) => {
      await openReplicationPage(page, airbyte);

      await test.step("save the stream as Full refresh | Overwrite", async () => {
        // Full refresh | Overwrite needs no PK or cursor, so the user can unselect any field of cities.
        await streamRow.selectSyncMode(page, "public", "cities", SyncMode.full_refresh, DestinationSyncMode.overwrite);

        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
        expect.soft(lastSavedStreamConfig(airbyte, "cities")).toMatchObject({
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite,
        });
      });

      await test.step("a disabled field shows as removed, and its stream as changed, until I save", async () => {
        await openReplicationPage(page, airbyte);
        await streamRow.toggleExpandCollapse(page, "public", "cities");

        await streamRow.toggleFieldSync(page, "public", "cities", "city", false);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(true);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "removed")).toBe(true);

        // Saving clears both styles.
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(false);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "removed")).toBe(false);
      });

      await test.step("an enabled field shows as added, and its stream as changed, until I save", async () => {
        await openReplicationPage(page, airbyte);
        await streamRow.toggleExpandCollapse(page, "public", "cities");

        await streamRow.toggleFieldSync(page, "public", "cities", "city", true);
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(true);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "added")).toBe(true);

        // Saving clears both styles.
        await streamsTable.clickSaveChangesButton(page);
        await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
        expect.soft(await streamRow.hasStreamStyle(page, "public", "cities", "changed")).toBe(false);
        expect.soft(await streamRow.hasFieldStyle(page, "public", "cities", "city", "added")).toBe(false);
      });
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
test.describe(() => {
  // The server keeps a deleted connection with the status "deprecated". Every stream is selected. users is
  // Incremental | Append + Deduped with the PK id and the cursor email, so its PK and cursor controls show.
  test.use({
    airbyteScenario: scenario({
      connections: [
        postgresConnection.with({
          status: "deprecated",
          catalog: catalogs.postgres({
            selected: true,
            streams: {
              users: {
                syncMode: SyncMode.incremental,
                destinationSyncMode: DestinationSyncMode.append_dedup,
                primaryKey: [["id"]],
                cursorField: ["email"],
              },
            },
          }),
        }),
      ],
    }),
  });

  test("view the streams of a deleted connection as read-only", async ({ page, airbyte }) => {
    await openReplicationPage(page, airbyte);

    await test.step("the name filter and the tabs still work", async () => {
      expect.soft(await streamsTable.isFilterInputEnabled(page)).toBe(true);

      await streamsTable.filterByStreamName(page, "users");
      expect.soft(await streamRow.isStreamVisible(page, "public", "users")).toBe(true);

      await streamsTable.filterByStreamName(page, "userss");
      expect.soft(await streamsTable.hasNoMatchingStreamsMessage(page)).toBe(true);

      await streamsTable.clearFilterByStreamName(page);

      expect.soft(await streamsTable.isFilterTabEnabled(page, "all")).toBe(true);
      expect.soft(await streamsTable.isFilterTabEnabled(page, "enabledStreams")).toBe(true);
      expect.soft(await streamsTable.isFilterTabEnabled(page, "disabledStreams")).toBe(true);

      await streamsTable.clickFilterTab(page, "enabledStreams");
      expect.soft(await streamRow.isStreamVisible(page, "public", "users")).toBe(true);
      expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "users")).toBe(true);

      // Every stream is enabled, so the disabled streams tab is empty.
      await streamsTable.clickFilterTab(page, "disabledStreams");
      expect.soft(await streamsTable.hasNoStreamsMessage(page)).toBe(true);

      await streamsTable.clickFilterTab(page, "all");
    });

    await test.step("the refresh schema button cannot be used", async () => {
      expect.soft(await streamsTable.refreshSchemaButtonExists(page)).toBe(true);
      expect.soft(await streamsTable.isRefreshSchemaButtonEnabled(page)).toBe(false);
    });

    await test.step("the expand and collapse all button can be used", async () => {
      expect.soft(await streamsTable.expandCollapseAllStreamsButtonExists(page)).toBe(true);
      expect.soft(await streamsTable.isExpandCollapseAllStreamsButtonEnabled(page)).toBe(true);
    });

    await test.step("the namespace checkbox cannot be changed", async () => {
      expect.soft(await streamsTable.isNamespaceCheckboxEnabled(page)).toBe(false);
    });

    await test.step("the stream checkbox cannot be changed", async () => {
      expect.soft(await streamRow.isStreamSyncCheckboxDisabled(page, "public", "users")).toBe(true);
    });

    await test.step("the field checkboxes cannot be changed", async () => {
      await streamRow.toggleExpandCollapse(page, "public", "users");

      // name is not the PK or the cursor. The checkbox of a PK or cursor field is disabled on every connection, so
      // only a field like name shows that the deleted connection is read-only.
      expect.soft(await streamRow.isFieldSyncCheckboxDisabled(page, "public", "users", "name")).toBe(true);
    });

    await test.step("the expand button of a stream can be used", async () => {
      expect.soft(await streamRow.isExpandButtonEnabled(page, "public", "users")).toBe(true);
    });

    await test.step("the sync mode cannot be changed", async () => {
      expect.soft(await streamRow.isSyncModeDropdownDisabled(page, "public", "users")).toBe(true);
    });

    await test.step("the PK and the cursor cannot be changed", async () => {
      expect.soft(await streamRow.isPKComboboxDisabled(page, "public", "users")).toBe(true);
      expect.soft(await streamRow.isCursorComboboxDisabled(page, "public", "users")).toBe(true);
    });
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
 * Uses 3 streams: users, cities, cars. No stream is selected at the start.
 */
test.describe(() => {
  test.use({ airbyteScenario: postgresScenario({ selected: false }) });

  test("filter streams by tab and by name", async ({ page, airbyte }) => {
    await openReplicationPage(page, airbyte);

    await test.step("the All tab shows every stream, enabled or not", async () => {
      await streamsTable.clickFilterTab(page, "all");

      expect.soft(await streamRow.isStreamVisible(page, "public", "users")).toBe(true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cities")).toBe(true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cars")).toBe(true);

      // An enabled stream stays in the "All" tab.
      await streamRow.toggleStreamSync(page, "public", "cars", true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cars")).toBe(true);
    });

    await test.step("the namespace row counts every stream", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(page);
      // The header cell shows the namespace name and then the count, with no space between them. The count is the
      // message "form.amountOfStreams" when the tab shows every stream, else "form.amountOfCountedStreamsOutOfTotal".
      expect.soft(countText).toBe("public3 streams");
    });

    await test.step("the Enabled streams tab shows only enabled streams", async () => {
      await streamsTable.clickFilterTab(page, "enabledStreams");

      expect.soft(await streamRow.isStreamVisible(page, "public", "users")).toBe(false);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cities")).toBe(false);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cars")).toBe(true);
      expect.soft(await streamRow.isStreamSyncEnabled(page, "public", "cars")).toBe(true);
    });

    await test.step("the name filter works in the Enabled streams tab", async () => {
      await streamsTable.filterByStreamName(page, "cars");
      expect.soft(await streamRow.isStreamVisible(page, "public", "cars")).toBe(true);

      await streamsTable.filterByStreamName(page, "carss");
      expect.soft(await streamsTable.hasNoMatchingStreamsMessage(page)).toBe(true);

      await streamsTable.clearFilterByStreamName(page);
    });

    await test.step("the namespace row counts the enabled streams out of all streams", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(page);
      expect.soft(countText).toBe("public1 / 3 streams");
    });

    await test.step("the Enabled streams tab is empty when no stream is enabled", async () => {
      await streamRow.toggleStreamSync(page, "public", "cars", false);

      expect.soft(await streamsTable.isNamespaceCellEmpty(page)).toBe(true);
      expect.soft(await streamsTable.hasNoStreamsMessage(page)).toBe(true);
    });

    await test.step("the Disabled streams tab shows only disabled streams", async () => {
      await streamsTable.clickFilterTab(page, "disabledStreams");

      expect.soft(await streamRow.isStreamVisible(page, "public", "users")).toBe(true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cities")).toBe(true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cars")).toBe(true);
    });

    await test.step("the name filter works in the Disabled streams tab", async () => {
      // An enabled stream leaves the "Disabled streams" tab.
      await streamRow.toggleStreamSync(page, "public", "cities", true);
      expect.soft(await streamRow.isStreamVisible(page, "public", "cities")).toBe(false);

      // cities is now enabled, so a filter for it matches nothing in this tab.
      await streamsTable.filterByStreamName(page, "cities");
      expect.soft(await streamsTable.isNamespaceCellEmpty(page)).toBe(true);
      expect.soft(await streamsTable.hasNoMatchingStreamsMessage(page)).toBe(true);

      await streamsTable.clearFilterByStreamName(page);
    });

    await test.step("the namespace row counts the disabled streams out of all streams", async () => {
      const countText = await streamsTable.getNamespaceStreamCount(page);
      expect.soft(countText).toBe("public2 / 3 streams");
    });
  });
});
