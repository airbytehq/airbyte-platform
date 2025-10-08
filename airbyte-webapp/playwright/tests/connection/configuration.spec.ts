import { test, expect } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/generated/AirbyteClient.schemas";

import { connectionAPI, connectionForm, connectionTestScaffold, connectionTestHelpers } from "../../helpers/connection";
import {
  apiInterceptors,
  connectionSettings,
  connectionDeletion,
  connectionWorkflows,
} from "../../helpers/connectionConfiguration";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection Configuration", () => {
  let workspaceId: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.describe("Sync frequency - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;

    test.beforeAll(async ({ request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-e2e");
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
    });

    test("should default to manual schedule", async ({ page }) => {
      const getConnectionRequests = await apiInterceptors.setupGetConnectionIntercept(page);
      await connectionSettings.navigateAndWaitForConnection(page, testData.connection, getConnectionRequests);

      const scheduleTypeButton = page.locator('[data-testid="schedule-type-listbox-button"]');
      await expect(scheduleTypeButton).toContainText("Manual", { timeout: 10000 });
    });

    test("should set cron as schedule type", async ({ page }) => {
      const requestBody = await connectionWorkflows.updateConnection(page, testData.connection, async (page) => {
        await connectionForm.selectScheduleType(page, "Cron");
      });

      expect(requestBody.scheduleType).toBe("cron");
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((requestBody.scheduleData as any).cron).toEqual({
        cronTimeZone: "UTC",
        cronExpression: "0 0 12 * * ?",
      });
    });

    test("should set hourly as schedule type", async ({ page }) => {
      const requestBody = await connectionWorkflows.updateConnection(page, testData.connection, async (page) => {
        await connectionForm.selectScheduleType(page, "Scheduled");
        await connectionForm.selectBasicScheduleData(page, "1-hours");
      });

      expect(requestBody.scheduleType).toBe("basic");
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((requestBody.scheduleData as any).basicSchedule).toEqual({
        timeUnit: "hours",
        units: 1,
      });
    });
  });

  test.describe("Destination namespace - Postgres → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;

    test.beforeAll(async ({ request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres");
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
    });

    test("should set destination namespace with custom format option", async ({ page }) => {
      await connectionWorkflows.updateNamespaceCustom(
        page,
        testData.connection,
        "_DestinationNamespaceCustomFormat",
        "public_DestinationNamespaceCustomFormat",
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
    });

    test("should show source namespace in preview for source-defined option", async ({ page }) => {
      await connectionSettings.navigateAndWaitForForm(page, testData.connection);
      await connectionForm.toggleAdvancedSettings(page);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreview(page, "source-namespace-preview", "public");
    });
  });

  test.describe("Destination namespace - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;

    test.beforeAll(async ({ request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-postgres");
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
    });

    test("should set custom namespace and show empty source namespace in preview", async ({ page }) => {
      await connectionWorkflows.updateNamespaceCustom(
        page,
        testData.connection,
        "_DestinationNamespaceCustomFormat",
        "_DestinationNamespaceCustomFormat", // PokeAPI has no source namespace, so preview shows only custom value
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
    });

    test("should not show source namespace preview for source-defined option", async ({ page }) => {
      await connectionSettings.navigateAndWaitForForm(page, testData.connection);
      await connectionForm.toggleAdvancedSettings(page);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreviewNotVisible(page, "source-namespace-preview");
    });

    test("should set destination default namespace option", async ({ page }) => {
      await connectionWorkflows.updateNamespaceDestinationDefault(page, testData.connection);
    });
  });

  test.describe("Destination prefix - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;

    test.beforeAll(async ({ request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-postgres");
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
    });

    test("should add destination prefix and set custom namespace format", async ({ page }) => {
      const requestBody = await connectionWorkflows.updateConnection(
        page,
        testData.connection,
        async (page) => {
          await connectionForm.setStreamPrefix(page, "auto_test");
          await connectionForm.setupDestinationNamespaceCustomFormat(page, "_test");
          await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
        },
        { toggleAdvanced: true }
      );

      expect(requestBody.prefix).toBe("auto_test");
      expect(requestBody.namespaceDefinition).toBe("customformat");
      expect(requestBody.namespaceFormat).toBe(`\${SOURCE_NAMESPACE}_test`);
    });

    test("should remove destination prefix", async ({ page, request }) => {
      // First, set prefix via API
      await connectionAPI.update(request, testData.connection.connectionId, {
        prefix: "auto_test",
      });

      const requestBody = await connectionWorkflows.updateConnection(
        page,
        testData.connection,
        async (page) => {
          // Verify prefix is initially present
          await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
          // Clear it
          await connectionForm.clearStreamPrefix(page);
          // Verify it's gone
          await connectionForm.verifyPreviewNotVisible(page, "stream-prefix-preview");
        },
        { toggleAdvanced: true }
      );

      expect(requestBody.prefix).toBe("");
    });
  });

  test.describe("Settings page", () => {
    test("should delete connection", async ({ page, request }) => {
      const testResources = await connectionTestHelpers.setupConnectionTest(request, workspaceId);
      const connection = await connectionAPI.create(request, testResources.source, testResources.destination, {
        enableAllStreams: true,
      });

      await connectionDeletion.deleteConnection(page, workspaceId, connection);

      // Cleanup source and destination only (connection already deleted)
      await connectionTestHelpers.cleanupConnectionTest(request, {
        sourceId: testResources.sourceId,
        destinationId: testResources.destinationId,
      });
    });
  });

  test.describe("Deleted connection", () => {
    let connection: WebBackendConnectionRead;
    let sourceId: string;
    let destinationId: string;

    test.beforeEach(async ({ request }) => {
      // Create a new connection for each test
      const testResources = await connectionTestHelpers.setupPokeApiPostgresConnectionTest(
        request,
        workspaceId,
        "PokeAPI source",
        "Postgres destination"
      );

      sourceId = testResources.sourceId;
      destinationId = testResources.destinationId;

      // Create connection with streams enabled
      connection = await connectionAPI.create(request, testResources.source, testResources.destination, {
        enableAllStreams: true,
      });

      // Immediately delete the connection
      await connectionAPI.delete(request, connection.connectionId);
    });

    test.afterEach(async ({ request }) => {
      // Clean up source and destination (connection already deleted in beforeEach)
      await connectionTestHelpers.cleanupConnectionTest(request, {
        sourceId,
        destinationId,
      });
    });

    test("should not be listed on connection list page", async ({ page }) => {
      // Navigate to connections list page
      await page.goto(`/workspaces/${workspaceId}/connections`, { timeout: 20000 });

      // Verify the deleted connection is not in the list
      const connectionNameCell = page.locator("td").filter({ hasText: connection.name });
      await expect(connectionNameCell).not.toBeVisible();
    });

    test("should show deleted message on timeline page", async ({ page }) => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "timeline");
      await connectionDeletion.verifyDeletedMessage(page);
    });

    test("should disable sync controls on timeline page", async ({ page }) => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "timeline");

      await connectionSettings.verifyElementDisabled(page, "connection-status-switch");
      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });

    test("should disable all form fields on settings page", async ({ page }) => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "settings");
      await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });

      await connectionForm.toggleAdvancedSettings(page);

      await connectionSettings.verifyElementDisabled(page, "connectionName");
      await connectionSettings.verifyElementDisabled(page, "schedule-type-listbox-button");
      await connectionSettings.verifyElementDisabled(page, "namespace-definition-listbox-button");
      await connectionSettings.verifyElementDisabled(page, "stream-prefix-input");
      await connectionSettings.verifyElementDisabled(page, "nonBreakingChangesPreference-listbox-button");
    });

    test("should not show reset and delete buttons on settings page", async ({ page }) => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "settings");
      await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });

      await connectionSettings.verifyElementNotVisible(page, '[data-testid="resetDataButton"]');
      await connectionSettings.verifyElementNotVisible(page, '[data-id="open-delete-modal"]');
    });
  });

  test.describe("Disabled connection", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;

    test.beforeEach(async ({ request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        status: "inactive",
      });
    });

    test.afterEach(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
    });

    test("should show streams table", async ({ page }) => {
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}`, {
        timeout: 20000,
      });

      // Verify the "users" stream is visible in the streams list
      const usersStream = page.locator('[data-testid="streams-list-name-cell-content"]').filter({ hasText: "users" });
      await expect(usersStream).toBeVisible({ timeout: 10000 });
    });

    test("should not allow triggering a sync", async ({ page }) => {
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}`, {
        timeout: 20000,
      });

      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });

    test("should allow editing connection and refreshing schema", async ({ page }) => {
      // Verify refresh schema button is enabled on replication page
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`, {
        timeout: 20000,
      });

      const refreshSchemaButton = page.locator('button[data-testid="refresh-schema-btn"]');
      await expect(refreshSchemaButton).toBeEnabled({ timeout: 10000 });

      // Update schedule via settings
      const requestBody = await connectionWorkflows.updateConnection(page, testData.connection, async (page) => {
        await connectionForm.selectScheduleType(page, "Scheduled");
      });

      expect(requestBody.scheduleType).toBe("basic");
    });
  });
});
