import type { MockAirbyte } from "../../helpers/mockApi";
import type { Page } from "@playwright/test";

import {
  customNamespaceScenario,
  deletionScenario,
  deletedConnectionScenario,
  disabledConnectionScenario,
  fakerExistingConnectionScenario,
  pokeConnectionScenario,
  pokePostgresConnectionScenario,
  postgresConnectionScenario,
  prefixedConnectionScenario,
} from "./configuration.scenarios";
import { connectionForm } from "../../helpers/connection";
import { connectionSettings, connectionDeletion } from "../../helpers/connectionConfiguration";
import { test, expect } from "../../helpers/frontendTest";

async function openSettings(page: Page, airbyte: MockAirbyte, advanced = false) {
  const connection = airbyte.connections[0];
  await page.goto(`/workspaces/${airbyte.workspace.workspaceId}/connections/${connection.connectionId}/settings`);
  await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });
  if (advanced) {
    await connectionForm.toggleAdvancedSettings(page);
  }
}

async function saveAndReload(page: Page, airbyte: MockAirbyte, advanced = false) {
  await connectionForm.submit(page);
  await connectionForm.verifySuccessNotification(page);
  expect(airbyte.updates).toHaveLength(1);
  const connection = airbyte.connections[0];
  expect(airbyte.updates[0]).toMatchObject({ connectionId: connection.connectionId, skipReset: true });

  await page.reload();
  await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });
  if (advanced) {
    await connectionForm.toggleAdvancedSettings(page);
  }
}

test.describe("Connection Configuration - mocked API", () => {
  test.use({ airbyteScenario: fakerExistingConnectionScenario });

  test("saves a cron schedule and displays it after reload", async ({ page, airbyte }) => {
    await openSettings(page, airbyte);
    await page.getByTestId("schedule-type-listbox-button").click();
    await page.getByTestId("cron-option").click();
    await saveAndReload(page, airbyte);
    expect(airbyte.updates[0].scheduleType).toBe("cron");
    expect(airbyte.updates[0].scheduleData).toEqual({ cron: { cronTimeZone: "UTC", cronExpression: "0 0 12 * * ?" } });
    await expect(page.getByTestId("schedule-type-listbox-button")).toContainText("Cron");
    await expect(page.getByTestId("cronExpression")).toHaveValue("0 0 12 * * ?");
    await expect(page.getByRole("button", { name: "UTC", exact: true })).toBeVisible();
  });

  test("saves an hourly schedule and displays it after reload", async ({ page, airbyte }) => {
    await openSettings(page, airbyte);
    await page.getByTestId("schedule-type-listbox-button").click();
    await page.getByTestId("scheduled-option").click();
    await page.getByTestId("basic-schedule-listbox-button").click();
    await page.getByTestId("frequency-1-hours-option").click();
    await saveAndReload(page, airbyte);
    expect(airbyte.updates[0].scheduleType).toBe("basic");
    expect(airbyte.updates[0].scheduleData).toEqual({ basicSchedule: { timeUnit: "hours", units: 1 } });
    await expect(page.getByTestId("schedule-type-listbox-button")).toContainText("Scheduled");
    await expect(page.getByTestId("basic-schedule-listbox-button")).toContainText("Every 1 hour");
  });

  test.describe("Sync frequency - PokeAPI → E2E", () => {
    test.use({ airbyteScenario: pokeConnectionScenario });
    test("should default to manual schedule", async ({ page, airbyte }) => {
      await openSettings(page, airbyte);
      await expect(page.getByTestId("schedule-type-listbox-button")).toContainText("Manual");
    });
  });

  test.describe("Destination namespace - Postgres → Postgres", () => {
    test.use({ airbyteScenario: postgresConnectionScenario });
    test("should set destination namespace with custom format option", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_DestinationNamespaceCustomFormat");
      await connectionForm.verifyPreview(page, "custom-namespace-preview", "public_DestinationNamespaceCustomFormat");
      await saveAndReload(page, airbyte, true);
      expect(airbyte.updates[0]).toMatchObject({
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`,
      });
      await expect(page.getByTestId("namespace-definition-custom-format-input")).toHaveValue(
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
      await connectionForm.verifyPreview(page, "custom-namespace-preview", "public_DestinationNamespaceCustomFormat");
    });
    test("should show source namespace in preview for source-defined option", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreview(page, "source-namespace-preview", "public");
      await saveAndReload(page, airbyte, true);
      expect(airbyte.updates[0].namespaceDefinition).toBe("source");
      await connectionForm.verifyPreview(page, "source-namespace-preview", "public");
    });
  });

  test.describe("Destination namespace - PokeAPI → Postgres", () => {
    test.use({ airbyteScenario: pokePostgresConnectionScenario });
    test("should set custom namespace and show empty source namespace in preview", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_DestinationNamespaceCustomFormat");
      await connectionForm.verifyPreview(page, "custom-namespace-preview", "_DestinationNamespaceCustomFormat");
      await saveAndReload(page, airbyte, true);
      expect(airbyte.updates[0]).toMatchObject({
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`,
      });
      await expect(page.getByTestId("namespace-definition-custom-format-input")).toHaveValue(
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
      await connectionForm.verifyPreview(page, "custom-namespace-preview", "_DestinationNamespaceCustomFormat");
    });
    test("should not show source namespace preview for source-defined option", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreviewNotVisible(page, "source-namespace-preview");
      await saveAndReload(page, airbyte, true);
      expect(airbyte.updates[0].namespaceDefinition).toBe("source");
      await expect(page.getByTestId("namespace-definition-listbox-button")).toContainText("Source");
      await connectionForm.verifyPreviewNotVisible(page, "source-namespace-preview");
    });
    test.describe("Initially custom namespace", () => {
      test.use({ airbyteScenario: customNamespaceScenario });
      test("should set destination default namespace option", async ({ page, airbyte }) => {
        await openSettings(page, airbyte, true);
        await connectionForm.setupDestinationNamespaceDestinationFormat(page);
        await saveAndReload(page, airbyte, true);
        expect(airbyte.updates[0].namespaceDefinition).toBe("destination");
        await expect(page.getByTestId("namespace-definition-listbox-button")).toContainText("Destination");
      });
    });
  });

  test.describe("Destination prefix - PokeAPI → Postgres", () => {
    test.use({ airbyteScenario: pokePostgresConnectionScenario });
    test("should add destination prefix and set custom namespace format", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionForm.setStreamPrefix(page, "auto_test");
      await connectionForm.setupDestinationNamespaceCustomFormat(page, "_test");
      await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
      await saveAndReload(page, airbyte, true);
      expect(airbyte.updates[0]).toMatchObject({
        prefix: "auto_test",
        namespaceDefinition: "customformat",
        namespaceFormat: `\${SOURCE_NAMESPACE}_test`,
      });
      await expect(page.getByTestId("stream-prefix-input")).toHaveValue("auto_test");
      await expect(page.getByTestId("namespace-definition-custom-format-input")).toHaveValue(
        `\${SOURCE_NAMESPACE}_test`
      );
      await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
    });
    test.describe("Initially prefixed", () => {
      test.use({ airbyteScenario: prefixedConnectionScenario });
      test("should remove destination prefix", async ({ page, airbyte }) => {
        await openSettings(page, airbyte, true);
        await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
        await connectionForm.clearStreamPrefix(page);
        await connectionForm.verifyPreviewNotVisible(page, "stream-prefix-preview");
        await saveAndReload(page, airbyte, true);
        expect(airbyte.updates[0].prefix).toBe("");
        await expect(page.getByTestId("stream-prefix-input")).toHaveValue("");
        await connectionForm.verifyPreviewNotVisible(page, "stream-prefix-preview");
      });
    });
  });

  test.describe("Settings page", () => {
    test.use({ airbyteScenario: deletionScenario });
    test("should delete connection", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await connectionDeletion.deleteConnection(page, airbyte.workspace.workspaceId, connection);
      expect(airbyte.requests.filter(({ key }) => key === "POST /api/v1/connections/delete")).toEqual([
        { key: "POST /api/v1/connections/delete", body: { connectionId: connection.connectionId } },
      ]);
      expect(connection.status).toBe("deprecated");
      await page.reload();
      await expect(page.getByTestId("new-connection-button")).toBeVisible();
      await expect(page.locator("td").filter({ hasText: "Another connection" })).toBeVisible();
      await expect(page.locator("td").filter({ hasText: connection.name })).not.toBeVisible();
    });
  });

  test.describe("Deleted connection", () => {
    test.use({ airbyteScenario: deletedConnectionScenario });
    test("should not be listed on connection list page", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await page.goto(`/workspaces/${airbyte.workspace.workspaceId}/connections`);
      await expect(page.getByTestId("new-connection-button")).toBeVisible();
      await expect(page.locator("td").filter({ hasText: "Another connection" })).toBeVisible();
      await expect(page.locator("td").filter({ hasText: connection.name })).not.toBeVisible();
    });
    test("should show deleted message on timeline page", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await connectionDeletion.navigateToDeleted(
        page,
        airbyte.workspace.workspaceId,
        connection.connectionId,
        "timeline"
      );
      await connectionDeletion.verifyDeletedMessage(page);
    });
    test("should disable sync controls on timeline page", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await connectionDeletion.navigateToDeleted(
        page,
        airbyte.workspace.workspaceId,
        connection.connectionId,
        "timeline"
      );
      await connectionSettings.verifyElementDisabled(page, "connection-status-switch");
      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });
    test("should disable all form fields on settings page", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionSettings.verifyElementDisabled(page, "connectionName");
      await connectionSettings.verifyElementDisabled(page, "schedule-type-listbox-button");
      await connectionSettings.verifyElementDisabled(page, "stream-prefix-input");
      await connectionSettings.verifyElementDisabled(page, "nonBreakingChangesPreference-listbox-button");
    });
    test("should not show reset and delete buttons on settings page", async ({ page, airbyte }) => {
      await openSettings(page, airbyte, true);
      await connectionSettings.verifyElementNotVisible(page, '[data-testid="resetDataButton"]');
      await connectionSettings.verifyElementNotVisible(page, '[data-id="open-delete-modal"]');
    });
  });

  test.describe("Disabled connection", () => {
    test.use({ airbyteScenario: disabledConnectionScenario });
    test("should show streams table", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await page.goto(`/workspaces/${airbyte.workspace.workspaceId}/connections/${connection.connectionId}`);
      await expect(page.getByTestId("streams-list-name-cell-content").filter({ hasText: "users" })).toBeVisible();
    });
    test("should not allow triggering a sync", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await page.goto(`/workspaces/${airbyte.workspace.workspaceId}/connections/${connection.connectionId}`);
      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });
    test("should allow editing connection and refreshing schema", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await page.goto(
        `/workspaces/${airbyte.workspace.workspaceId}/connections/${connection.connectionId}/replication`
      );
      await expect(page.getByTestId("refresh-schema-btn")).toBeEnabled();
      await openSettings(page, airbyte);
      await connectionForm.selectScheduleType(page, "Scheduled");
      await saveAndReload(page, airbyte);
      expect(airbyte.updates[0].scheduleType).toBe("basic");
      await expect(page.getByTestId("schedule-type-listbox-button")).toContainText("Scheduled");
      expect(connection.status).toBe("inactive");
    });
  });
});
