import { test, expect, Page, BrowserContext } from "@playwright/test";
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

  test.describe.serial("Sync frequency - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      // Serial tests share a connection and page to avoid redundant setup overhead
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-e2e");
      context = await browser.newContext();
      page = await context.newPage();

      const getConnectionRequests = await apiInterceptors.setupGetConnectionIntercept(page);
      await connectionSettings.navigateAndWaitForConnection(page, testData.connection, getConnectionRequests);
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      await page.close();
      await context.close();
    });

    test("should default to manual schedule", async () => {
      // Already navigated in beforeAll
      const scheduleTypeButton = page.locator('[data-testid="schedule-type-listbox-button"]');
      await expect(scheduleTypeButton).toContainText("Manual", { timeout: 10000 });
    });

    test("should set cron as schedule type", async () => {
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

    test("should set hourly as schedule type", async () => {
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

  test.describe.serial("Destination namespace - Postgres → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres");
      context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      await page.close();
      await context.close();
    });

    test("should set destination namespace with custom format option", async () => {
      await connectionWorkflows.updateNamespaceCustom(
        page,
        testData.connection,
        "_DestinationNamespaceCustomFormat",
        "public_DestinationNamespaceCustomFormat",
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
    });

    test("should show source namespace in preview for source-defined option", async () => {
      await connectionSettings.navigateAndWaitForForm(page, testData.connection);
      await connectionForm.toggleAdvancedSettings(page);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreview(page, "source-namespace-preview", "public");
    });
  });

  test.describe.serial("Destination namespace - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-postgres");
      context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      await page.close();
      await context.close();
    });

    test("should set custom namespace and show empty source namespace in preview", async () => {
      // PokeAPI has no source namespace, so preview shows only custom value
      await connectionWorkflows.updateNamespaceCustom(
        page,
        testData.connection,
        "_DestinationNamespaceCustomFormat",
        "_DestinationNamespaceCustomFormat",
        `\${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat`
      );
    });

    test("should not show source namespace preview for source-defined option", async () => {
      await connectionSettings.navigateAndWaitForForm(page, testData.connection);
      await connectionForm.toggleAdvancedSettings(page);
      await connectionForm.setupDestinationNamespaceSourceFormat(page);
      await connectionForm.verifyPreviewNotVisible(page, "source-namespace-preview");
    });

    test("should set destination default namespace option", async () => {
      await connectionWorkflows.updateNamespaceDestinationDefault(page, testData.connection);
    });
  });

  test.describe.serial("Destination prefix - PokeAPI → Postgres", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "poke-postgres");
      context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      await page.close();
      await context.close();
    });

    test("should add destination prefix and set custom namespace format", async () => {
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

    test("should remove destination prefix", async ({ request }) => {
      // Set prefix via API so we can test removal
      await connectionAPI.update(request, testData.connection.connectionId, {
        prefix: "auto_test",
      });

      const requestBody = await connectionWorkflows.updateConnection(
        page,
        testData.connection,
        async (page) => {
          await connectionForm.verifyPreview(page, "stream-prefix-preview", "auto_test");
          await connectionForm.clearStreamPrefix(page);
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

  test.describe.serial("Deleted connection", () => {
    let connection: WebBackendConnectionRead;
    let sourceId: string;
    let destinationId: string;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      const testResources = await connectionTestHelpers.setupPokeApiPostgresConnectionTest(
        request,
        workspaceId,
        "PokeAPI source",
        "Postgres destination"
      );

      sourceId = testResources.sourceId;
      destinationId = testResources.destinationId;

      connection = await connectionAPI.create(request, testResources.source, testResources.destination, {
        enableAllStreams: true,
      });

      await connectionAPI.delete(request, connection.connectionId);

      context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterAll(async ({ request }) => {
      // Connection already deleted, only cleanup source and destination
      await connectionTestHelpers.cleanupConnectionTest(request, {
        sourceId,
        destinationId,
      });
      await page.close();
      await context.close();
    });

    test("should not be listed on connection list page", async () => {
      await page.goto(`/workspaces/${workspaceId}/connections`, { timeout: 20000 });

      const connectionNameCell = page.locator("td").filter({ hasText: connection.name });
      await expect(connectionNameCell).not.toBeVisible();
    });

    test("should show deleted message on timeline page", async () => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "timeline");
      await connectionDeletion.verifyDeletedMessage(page);
    });

    test("should disable sync controls on timeline page", async () => {
      // Continue from previous test - already on timeline page
      await connectionSettings.verifyElementDisabled(page, "connection-status-switch");
      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });

    test("should disable all form fields on settings page", async () => {
      await connectionDeletion.navigateToDeleted(page, workspaceId, connection.connectionId, "settings");
      await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });

      await connectionForm.toggleAdvancedSettings(page);

      await connectionSettings.verifyElementDisabled(page, "connectionName");
      await connectionSettings.verifyElementDisabled(page, "schedule-type-listbox-button");
      await connectionSettings.verifyElementDisabled(page, "namespace-definition-listbox-button");
      await connectionSettings.verifyElementDisabled(page, "stream-prefix-input");
      await connectionSettings.verifyElementDisabled(page, "nonBreakingChangesPreference-listbox-button");
    });

    test("should not show reset and delete buttons on settings page", async () => {
      // Continue from previous test - already on settings page with advanced settings open
      await connectionSettings.verifyElementNotVisible(page, '[data-testid="resetDataButton"]');
      await connectionSettings.verifyElementNotVisible(page, '[data-id="open-delete-modal"]');
    });
  });

  test.describe.serial("Disabled connection", () => {
    let testData: Awaited<ReturnType<typeof connectionTestScaffold.setupConnection>>;
    let page: Page;
    let context: BrowserContext;

    test.beforeAll(async ({ browser, request }) => {
      testData = await connectionTestScaffold.setupConnection(request, workspaceId, "postgres-postgres", {
        status: "inactive",
      });
      context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterAll(async ({ request }) => {
      await connectionTestScaffold.cleanupConnection(request, testData);
      await page.close();
      await context.close();
    });

    test("should show streams table", async () => {
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}`, {
        timeout: 20000,
      });

      const usersStream = page.locator('[data-testid="streams-list-name-cell-content"]').filter({ hasText: "users" });
      await expect(usersStream).toBeVisible({ timeout: 10000 });
    });

    test("should not allow triggering a sync", async () => {
      // Continue from previous test - already on connection page
      await connectionSettings.verifyElementDisabled(page, "manual-sync-button");
    });

    test("should allow editing connection and refreshing schema", async () => {
      await page.goto(`/workspaces/${workspaceId}/connections/${testData.connection.connectionId}/replication`, {
        timeout: 20000,
      });

      const refreshSchemaButton = page.locator('button[data-testid="refresh-schema-btn"]');
      await expect(refreshSchemaButton).toBeEnabled({ timeout: 10000 });

      const requestBody = await connectionWorkflows.updateConnection(page, testData.connection, async (page) => {
        await connectionForm.selectScheduleType(page, "Scheduled");
      });

      expect(requestBody.scheduleType).toBe("basic");
    });
  });
});
