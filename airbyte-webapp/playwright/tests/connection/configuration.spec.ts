/**
 * Connection Settings tests: the sync schedule, the destination namespace, the stream prefix, the delete of a
 * connection, and the settings of a deleted or a disabled connection.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it. The check for the "saved"
 * notification stays a normal `expect`, because the next step reloads the page and needs the save to be finished.
 */
import type { ConnectionHandle, MockAirbyte } from "../../mock";
import type { Page } from "@playwright/test";

import { connectionForm } from "../../helpers/connection";
import { connectionDeletion } from "../../helpers/connectionConfiguration";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

const pokeSource = source({ def: defs.pokeApiSource, name: "PokeAPI source" });
const e2eDestination = destination({ def: defs.e2eTestingDestination, name: "E2E Testing destination" });
const postgresDestination = destination({ def: defs.postgresDestination, name: "Postgres destination" });

/** A PokeAPI → E2E Testing connection with the pokemon stream selected. It syncs manually, the default of the store. */
const pokeConnection = connection({
  source: pokeSource,
  destination: e2eDestination,
  catalog: catalogs.pokeApi({ selected: true }),
});

/**
 * A PokeAPI → Postgres connection that writes to the default namespace of the destination, without a stream prefix.
 * The pokemon stream of PokeAPI has no namespace.
 */
const pokePostgresConnection = connection({
  source: pokeSource,
  destination: postgresDestination,
  catalog: catalogs.pokeApi({ selected: true }),
  namespaceDefinition: "destination",
  prefix: "",
});

const submitButton = (page: Page) => page.locator("button[type='submit']");
const scheduleTypeButton = (page: Page) => page.getByTestId("schedule-type-listbox-button");
const namespaceButton = (page: Page) => page.getByTestId("namespace-definition-listbox-button");
const customFormatInput = (page: Page) => page.getByTestId("namespace-definition-custom-format-input");
const streamPrefixInput = (page: Page) => page.getByTestId("stream-prefix-input");
const customNamespacePreview = (page: Page) => page.getByTestId("custom-namespace-preview");
const sourceNamespacePreview = (page: Page) => page.getByTestId("source-namespace-preview");
const streamPrefixPreview = (page: Page) => page.getByTestId("stream-prefix-preview");

/** Opens the advanced settings, which hold the namespace, the prefix, and the schema change preference. */
async function openAdvancedSettings(page: Page) {
  await page.getByTestId("advanced-settings-button").click();
  await expect(streamPrefixInput(page)).toBeVisible();
}

/** Opens the Settings tab of a connection and waits for the form. */
async function openSettings(page: Page, airbyte: MockAirbyte, ref: ConnectionHandle, { advanced = false } = {}) {
  await page.goto(airbyte.paths.connection(ref, "settings"));
  await expect(submitButton(page)).toBeVisible();
  if (advanced) {
    await openAdvancedSettings(page);
  }
}

/** Saves the settings form, waits for the "saved" notification, then reloads the page. */
async function saveAndReload(page: Page, { advanced = false } = {}) {
  await connectionForm.submit(page);
  await connectionForm.verifySuccessNotification(page);

  await page.reload();
  await expect(submitButton(page)).toBeVisible();
  if (advanced) {
    await openAdvancedSettings(page);
  }
}

/**
 * Sync schedule
 *
 * The start state is `pokeConnection`, which syncs manually.
 */
test.describe(() => {
  test.use({ airbyteScenario: scenario({ connections: [pokeConnection] }) });

  test("change the sync schedule to cron, then to every hour", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(pokeConnection);
    // The server describes a cron expression in words. The mock cannot compute the description, so the test gives it.
    airbyte.override("webBackendDescribeCronExpression", ({ body }) => ({
      cronExpression: body.cronExpression,
      description: "At 12:00 PM",
      nextExecutions: [1791028800, 1791115200],
    }));

    await test.step("open the Settings tab", async () => {
      await openSettings(page, airbyte, pokeConnection);
    });

    await test.step("the connection syncs manually", async () => {
      await expect.soft(scheduleTypeButton(page)).toContainText("Manual");
    });

    await test.step("select the Cron schedule type", async () => {
      await connectionForm.selectScheduleType(page, "Cron");
    });

    await test.step("the default cron expression and its description show", async () => {
      await expect.soft(page.getByTestId("cronExpression")).toHaveValue("0 0 12 * * ?");
      await expect.soft(page.getByText("At 12:00 PM")).toBeVisible();
    });

    await test.step("the webapp asks the server to describe the cron expression", async () => {
      expect.soft(airbyte.calls("webBackendDescribeCronExpression")).toContainEqual({ cronExpression: "0 0 12 * * ?" });
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page);
    });

    await test.step("the webapp saves the cron schedule", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(1);
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId,
        skipReset: true,
        scheduleType: "cron",
        scheduleData: { cron: { cronTimeZone: "UTC", cronExpression: "0 0 12 * * ?" } },
      });
    });

    await test.step("the saved cron schedule shows", async () => {
      await expect.soft(scheduleTypeButton(page)).toContainText("Cron");
      await expect.soft(page.getByTestId("cronExpression")).toHaveValue("0 0 12 * * ?");
      await expect.soft(page.getByRole("button", { name: "UTC", exact: true })).toBeVisible();
    });

    await test.step("select the Scheduled schedule type, every 1 hour", async () => {
      await connectionForm.selectScheduleType(page, "Scheduled");
      await page.getByTestId("basic-schedule-listbox-button").click();
      await page.getByTestId("frequency-1-hours-option").click();
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page);
    });

    await test.step("the webapp saves the hourly schedule", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(2);
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId,
        skipReset: true,
        scheduleType: "basic",
        scheduleData: { basicSchedule: { timeUnit: "hours", units: 1 } },
      });
    });

    await test.step("the saved hourly schedule shows", async () => {
      await expect.soft(scheduleTypeButton(page)).toContainText("Scheduled");
      await expect.soft(page.getByTestId("basic-schedule-listbox-button")).toContainText("Every 1 hour");
    });
  });
});

/**
 * Destination namespace, source with namespaces
 *
 * The start state is a Postgres → Postgres connection that writes to the default namespace of the destination. Its
 * streams are in the namespace "public".
 */
test.describe(() => {
  const postgresConnection = connection({
    source: source({ def: defs.postgresSource, name: "Postgres source" }),
    destination: postgresDestination,
    catalog: catalogs.postgres({ selected: true }),
    namespaceDefinition: "destination",
    prefix: "",
  });
  test.use({ airbyteScenario: scenario({ connections: [postgresConnection] }) });

  test("set the destination namespace when the source has namespaces", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(postgresConnection);

    await test.step("open the advanced settings", async () => {
      await openSettings(page, airbyte, postgresConnection, { advanced: true });
    });

    await test.step("select a custom format that adds a suffix to the source namespace", async () => {
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_DestinationNamespaceCustomFormat");
    });

    await test.step("the preview shows the source namespace with the suffix", async () => {
      await expect.soft(customNamespacePreview(page)).toContainText("public_DestinationNamespaceCustomFormat");
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the custom format", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(1);
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId,
        skipReset: true,
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`,
      });
    });

    await test.step("the saved custom format and its preview show", async () => {
      await expect.soft(customFormatInput(page)).toHaveValue(`\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`);
      await expect.soft(customNamespacePreview(page)).toContainText("public_DestinationNamespaceCustomFormat");
    });

    await test.step("select the namespace of the source", async () => {
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
    });

    await test.step("the preview shows the source namespace", async () => {
      await expect.soft(sourceNamespacePreview(page)).toContainText("public");
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the source namespace option", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(2);
      expect
        .soft(airbyte.calls.last("webBackendUpdateConnection"))
        .toMatchObject({ connectionId, skipReset: true, namespaceDefinition: "source" });
    });

    await test.step("the preview of the saved option shows the source namespace", async () => {
      await expect.soft(sourceNamespacePreview(page)).toContainText("public");
    });
  });
});

/**
 * Destination namespace, source without namespaces
 *
 * The start state is `pokePostgresConnection`. The pokemon stream of PokeAPI has no namespace.
 */
test.describe(() => {
  test.use({ airbyteScenario: scenario({ connections: [pokePostgresConnection] }) });

  test("set the destination namespace when the source has no namespaces", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(pokePostgresConnection);

    await test.step("open the advanced settings", async () => {
      await openSettings(page, airbyte, pokePostgresConnection, { advanced: true });
    });

    await test.step("select the namespace of the source", async () => {
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
    });

    await test.step("no source namespace preview shows, because the source has no namespace", async () => {
      await expect.soft(sourceNamespacePreview(page)).toBeHidden();
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the source namespace option", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(1);
      expect
        .soft(airbyte.calls.last("webBackendUpdateConnection"))
        .toMatchObject({ connectionId, skipReset: true, namespaceDefinition: "source" });
    });

    await test.step("the saved source namespace option shows, without a preview", async () => {
      await expect.soft(namespaceButton(page)).toContainText("Source");
      await expect.soft(sourceNamespacePreview(page)).toBeHidden();
    });

    await test.step("select a custom format that adds a suffix to the source namespace", async () => {
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_DestinationNamespaceCustomFormat");
    });

    await test.step("the preview shows only the suffix, because the source namespace is empty", async () => {
      await expect.soft(customNamespacePreview(page)).toContainText("_DestinationNamespaceCustomFormat");
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the custom format", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(2);
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId,
        skipReset: true,
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`,
      });
    });

    await test.step("the saved custom format and its preview show", async () => {
      await expect.soft(customFormatInput(page)).toHaveValue(`\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`);
      await expect.soft(customNamespacePreview(page)).toContainText("_DestinationNamespaceCustomFormat");
    });

    await test.step("select the default namespace of the destination", async () => {
      await connectionForm.setupDestinationNamespaceDestinationFormat(page);
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the destination namespace option", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(3);
      expect
        .soft(airbyte.calls.last("webBackendUpdateConnection"))
        .toMatchObject({ connectionId, skipReset: true, namespaceDefinition: "destination" });
    });

    await test.step("the saved destination namespace option shows", async () => {
      await expect.soft(namespaceButton(page)).toContainText("Destination");
    });
  });
});

/**
 * Stream prefix
 *
 * The start state is `pokePostgresConnection`, which has no stream prefix.
 */
test.describe(() => {
  test.use({ airbyteScenario: scenario({ connections: [pokePostgresConnection] }) });

  test("add and remove a stream prefix", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(pokePostgresConnection);

    await test.step("open the advanced settings", async () => {
      await openSettings(page, airbyte, pokePostgresConnection, { advanced: true });
    });

    await test.step("type a stream prefix and select a custom namespace format", async () => {
      await connectionForm.setStreamPrefix(page, "auto_test");
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_test");
    });

    await test.step("the preview shows the prefix", async () => {
      await expect.soft(streamPrefixPreview(page)).toContainText("auto_test");
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves the prefix and the custom format", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(1);
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId,
        skipReset: true,
        prefix: "auto_test",
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_test`,
      });
    });

    await test.step("the saved prefix, custom format and preview show", async () => {
      await expect.soft(streamPrefixInput(page)).toHaveValue("auto_test");
      await expect.soft(customFormatInput(page)).toHaveValue(`\${SOURCE_NAMESPACE}_test`);
      await expect.soft(streamPrefixPreview(page)).toContainText("auto_test");
    });

    await test.step("clear the stream prefix", async () => {
      await connectionForm.clearStreamPrefix(page);
    });

    await test.step("the prefix preview goes away", async () => {
      await expect.soft(streamPrefixPreview(page)).toBeHidden();
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page, { advanced: true });
    });

    await test.step("the webapp saves an empty prefix", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(2);
      expect
        .soft(airbyte.calls.last("webBackendUpdateConnection"))
        .toMatchObject({ connectionId, skipReset: true, prefix: "" });
    });

    await test.step("the prefix field is empty, without a preview", async () => {
      await expect.soft(streamPrefixInput(page)).toHaveValue("");
      await expect.soft(streamPrefixPreview(page)).toBeHidden();
    });
  });
});

/**
 * Delete a connection
 *
 * The start state is `pokeConnection` and a second connection, so that the connection list does not become empty.
 */
test.describe(() => {
  const anotherConnection = pokeConnection.copy({ name: "Another connection" });
  test.use({ airbyteScenario: scenario({ connections: [pokeConnection, anotherConnection] }) });

  test("delete a connection, then view it as read-only", async ({ page, airbyte }) => {
    const deleted = airbyte.store.connections.get(pokeConnection);

    await test.step("delete the connection from its Settings tab", async () => {
      await connectionDeletion.deleteConnection(page, airbyte.workspaceId, deleted);
    });

    await test.step("the webapp asks the server to delete the connection", async () => {
      expect.soft(airbyte.calls("deleteConnection")).toEqual([{ connectionId: deleted.connectionId }]);
    });

    await test.step("reload the connection list", async () => {
      await page.reload();
      await expect(page.getByTestId("new-connection-button")).toBeVisible();
    });

    await test.step("the list shows the other connection, but not the deleted one", async () => {
      await expect.soft(page.locator("td").filter({ hasText: "Another connection" })).toBeVisible();
      await expect.soft(page.locator("td").filter({ hasText: deleted.name })).toBeHidden();
    });

    await test.step("open the Timeline tab of the deleted connection", async () => {
      await page.goto(airbyte.paths.connection(pokeConnection, "timeline"));
    });

    await test.step("the deleted message shows, and the sync controls are disabled", async () => {
      await expect
        .soft(
          page
            .locator("div")
            .filter({ hasText: /^This connection has been deleted/ })
            .first()
        )
        .toBeVisible();
      await expect.soft(page.getByTestId("connection-status-switch")).toBeDisabled();
      await expect.soft(page.getByTestId("manual-sync-button")).toBeDisabled();
    });

    await test.step("open the advanced settings of the deleted connection", async () => {
      await openSettings(page, airbyte, pokeConnection, { advanced: true });
    });

    await test.step("every settings field is disabled", async () => {
      await expect.soft(page.getByTestId("connectionName")).toBeDisabled();
      await expect.soft(scheduleTypeButton(page)).toBeDisabled();
      await expect.soft(streamPrefixInput(page)).toBeDisabled();
      await expect.soft(page.getByTestId("nonBreakingChangesPreference-listbox-button")).toBeDisabled();
    });

    await test.step("the reset and delete buttons do not show", async () => {
      await expect.soft(page.getByTestId("resetDataButton")).toBeHidden();
      await expect.soft(page.locator("[data-id='open-delete-modal']")).toBeHidden();
    });
  });
});

/**
 * Disabled connection
 *
 * The start state is `pokeConnection`, disabled.
 */
test.describe(() => {
  const disabledConnection = pokeConnection.with({ status: "inactive" });
  test.use({ airbyteScenario: scenario({ connections: [disabledConnection] }) });

  test("edit the settings of a disabled connection", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(disabledConnection);

    await test.step("open the connection", async () => {
      await page.goto(airbyte.paths.connection(disabledConnection));
    });

    await test.step("the streams table shows, and the sync button is disabled", async () => {
      await expect
        .soft(page.getByTestId("streams-list-name-cell-content").filter({ hasText: "pokemon" }))
        .toBeVisible();
      await expect.soft(page.getByTestId("manual-sync-button")).toBeDisabled();
    });

    await test.step("open the Schema tab", async () => {
      await page.goto(airbyte.paths.connection(disabledConnection, "replication"));
    });

    await test.step("the refresh schema button is enabled", async () => {
      await expect.soft(page.getByTestId("refresh-schema-btn")).toBeEnabled();
    });

    await test.step("open the Settings tab, and select the Scheduled schedule type", async () => {
      await openSettings(page, airbyte, disabledConnection);
      await connectionForm.selectScheduleType(page, "Scheduled");
    });

    await test.step("save the settings and reload the page", async () => {
      await saveAndReload(page);
    });

    await test.step("the webapp saves the schedule", async () => {
      expect.soft(airbyte.calls("webBackendUpdateConnection")).toHaveLength(1);
      expect
        .soft(airbyte.calls.last("webBackendUpdateConnection"))
        .toMatchObject({ connectionId, skipReset: true, scheduleType: "basic" });
    });

    await test.step("the saved schedule shows, and the connection stays disabled", async () => {
      await expect.soft(scheduleTypeButton(page)).toContainText("Scheduled");
      await expect.soft(page.getByTestId("connection-status-switch")).not.toBeChecked();
    });
  });
});
