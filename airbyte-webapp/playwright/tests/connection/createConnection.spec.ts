import type { MockAirbyte } from "../../helpers/mockApi";
import type { Page } from "@playwright/test";

import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

import {
  navigateToConnectionConfig,
  selectSyncMode,
  filterAndFindStream,
  completeConnectionCreation,
  namespaceHelpers,
} from "../../helpers/connectionCreation";
import { test, expect } from "../../helpers/frontendTest";
import { createMockPostgresCatalog } from "../../helpers/mocks";

// Connector definitions and catalog data describe the scenario without configuring routes.
test.use({
  airbyteScenario: {
    source: {
      definition: {
        sourceDefinitionId: sourceIds.Postgres,
        name: "Postgres",
        dockerRepository: "airbyte/source-postgres",
        dockerImageTag: "1.0.0",
      },
      name: "Test Postgres source",
      connectionConfiguration: {},
      specification: { type: "object", properties: {} },
      catalog: createMockPostgresCatalog(),
    },
    destination: {
      definition: {
        destinationDefinitionId: destinationIds.Postgres,
        name: "Postgres",
        dockerRepository: "airbyte/destination-postgres",
        dockerImageTag: "1.0.0",
        documentationUrl: "",
      },
      name: "Test Postgres destination",
      connectionConfiguration: {},
      specification: { type: "object", properties: {} },
      supportedSyncModes: ["overwrite", "append", "append_dedup"],
    },
    connections: [],
  },
});

async function openConfiguration(page: Page, airbyte: MockAirbyte) {
  await navigateToConnectionConfig(page, airbyte.workspace.workspaceId, airbyte.source, airbyte.destination, {
    setupDiscoverSchemaIntercept: false,
  });
}

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    window._e2ePlaywrightEnvironment = true;
    window._e2eOverwrites = { asyncSchemaDiscovery: true };
  });
});

test.describe("Connection - Create new connection", () => {
  test.describe("Set up connection", () => {
    test.describe("From connection page", () => {
      test.beforeEach(async ({ page, airbyte }) => {
        airbyte.connections.push(airbyte.connection);
        await page.goto(`/workspaces/${airbyte.workspace.workspaceId}/connections`);
        await page.getByTestId("new-connection-button").click();
      });

      test("should open 'New connection' page", async ({ page, airbyte }) => {
        await expect(page).toHaveURL(/\/connections\/new-connection$/);
        await expect
          .poll(() => airbyte.requests.filter(({ key }) => key === "POST /api/v1/sources/list").length)
          .toBeGreaterThanOrEqual(1);
        await expect
          .poll(
            () =>
              airbyte.requests.filter(({ key }) => key === "POST /api/v1/source_definitions/list_for_workspace").length
          )
          .toBeGreaterThanOrEqual(1);
      });

      test("should select existing Source", async ({ page, airbyte }) => {
        await expect(page.getByTestId("radio-button-tile-sourceType-existing")).toBeChecked();
        await expect(page.getByTestId("radio-button-tile-sourceType-new")).not.toBeChecked();
        await page.getByTestId(`select-existing-source-${airbyte.source.name}`).click();
        await expect(page.getByTestId("radio-button-tile-destinationType-existing")).toBeVisible();
      });

      test("should select existing Destination", async ({ page, airbyte }) => {
        await page.getByTestId(`select-existing-source-${airbyte.source.name}`).click();
        await expect(page.getByTestId("radio-button-tile-destinationType-existing")).toBeChecked();
        await expect(page.getByTestId("radio-button-tile-destinationType-new")).not.toBeChecked();
        await page.getByTestId(`select-existing-destination-${airbyte.destination.name}`).click();
        await expect
          .poll(() => airbyte.requests.filter(({ key }) => key === "POST /api/v1/commands/run/discover").length)
          .toBeGreaterThanOrEqual(1);
      });

      test("should redirect to 'New connection' configuration page with stream table", async ({ page, airbyte }) => {
        await page.getByTestId(`select-existing-source-${airbyte.source.name}`).click();
        await page.getByTestId(`select-existing-destination-${airbyte.destination.name}`).click();
        await expect(page).toHaveURL(/\/connections\/new-connection\/configure/);
        const streamsTable = page.getByTestId("sync-catalog-table");
        await expect(streamsTable).toBeVisible();
        await streamsTable.scrollIntoViewIfNeeded();
        await expect(page.locator('[data-testid^="row-depth-1-stream"]').first()).toBeVisible();
      });
    });
  });

  test.describe("Streams table", () => {
    test.beforeEach(async ({ page, airbyte }) => {
      await openConfiguration(page, airbyte);
    });

    test("should have no streams checked by default", async ({ page }) => {
      // Verify namespace checkbox is not checked
      const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
      await expect(namespaceCheckbox).not.toBeChecked({ timeout: 10000 });

      // Verify all streams in the public namespace are not enabled
      const streamCheckboxes = page.locator(
        '[data-testid^="row-depth-1-stream"] input[data-testid="sync-stream-checkbox"]'
      );
      const checkboxCount = await streamCheckboxes.count();
      for (let i = 0; i < checkboxCount; i++) {
        await expect(streamCheckboxes.nth(i)).not.toBeChecked({ timeout: 5000 });
      }
    });

    test("should verify namespace row", async ({ page }) => {
      // Verify namespace checkbox is enabled but not checked
      const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
      await expect(namespaceCheckbox).toBeEnabled({ timeout: 10000 });
      await expect(namespaceCheckbox).not.toBeChecked({ timeout: 10000 });

      // Verify namespace row elements using helper
      return namespaceHelpers.verifyNamespaceRow(page, "public");
    });

    test("should show 'no selected streams' error", async ({ page }) => {
      // Verify no selected streams error is displayed
      await expect(page.locator("text=Select at least 1 stream to sync.")).toBeVisible({ timeout: 10000 });

      // Verify all streams are not enabled
      const streamCheckboxes = page.locator(
        '[data-testid^="row-depth-1-stream"] input[data-testid="sync-stream-checkbox"]'
      );
      const checkboxCount = await streamCheckboxes.count();
      for (let i = 0; i < checkboxCount; i++) {
        await expect(streamCheckboxes.nth(i)).not.toBeChecked({ timeout: 5000 });
      }

      // Verify next button is disabled
      return expect(page.locator('[data-testid="next-creation-page"]')).toBeDisabled({ timeout: 10000 });
    });

    test("should NOT show 'no selected streams' error", async ({ page }) => {
      // Toggle namespace checkbox to enable all streams
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", true);

      // Verify all streams in namespace are now enabled
      const streamCheckboxes = page.locator(
        '[data-testid^="row-depth-1-stream"] input[data-testid="sync-stream-checkbox"]'
      );
      const checkboxCount = await streamCheckboxes.count();
      for (let i = 0; i < checkboxCount; i++) {
        await expect(streamCheckboxes.nth(i)).toBeChecked({ timeout: 5000 });
      }

      // Verify no selected streams error is not displayed
      await expect(page.locator("text=Select at least 1 stream to sync.")).not.toBeVisible({ timeout: 10000 });

      // Note: Next button may still be disabled due to missing cursor/sync mode configuration
      await expect(page.locator('[data-testid="next-creation-page"]')).toBeDisabled({ timeout: 10000 });

      // Toggle namespace checkbox back to uncheck all streams
      return namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);
    });

    test("should not replace refresh schema button with form controls", async ({ page }) => {
      // Verify refresh schema button exists
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });

      // Verify expand/collapse all streams button exists
      await expect(page.locator('button[data-testid="expand-collapse-all-streams-btn"]')).toBeVisible({
        timeout: 10000,
      });

      // Enable all streams by checking namespace checkbox
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", true);

      // Verify buttons still exist after enabling streams
      await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible({ timeout: 10000 });
      await expect(page.locator('button[data-testid="expand-collapse-all-streams-btn"]')).toBeVisible({
        timeout: 10000,
      });

      // Toggle namespace checkbox back to uncheck all streams
      return namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);
    });

    test("should enable all streams in namespace", async ({ page }) => {
      // Toggle namespace checkbox to enable all streams
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", true);

      // Verify all streams are enabled
      const streamCheckboxes = page.locator(
        '[data-testid^="row-depth-1-stream"] input[data-testid="sync-stream-checkbox"]'
      );
      const checkboxCount = await streamCheckboxes.count();
      for (let i = 0; i < checkboxCount; i++) {
        await expect(streamCheckboxes.nth(i)).toBeChecked({ timeout: 5000 });
      }

      // Toggle namespace checkbox to disable all streams
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);

      // Verify all streams are disabled
      for (let i = 0; i < checkboxCount; i++) {
        await expect(streamCheckboxes.nth(i)).not.toBeChecked({ timeout: 5000 });
      }
    });
  });

  test.describe("Stream", () => {
    test.beforeEach(async ({ page, airbyte }) => {
      await openConfiguration(page, airbyte);
    });

    test("should enable and disable stream", async ({ page }) => {
      // Filter by stream name to find the users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Verify namespace checkbox is disabled when filtering
      const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
      await expect(namespaceCheckbox).toBeDisabled({ timeout: 10000 });

      // Verify stream has disabled style initially
      await expect(usersStreamRow).toHaveClass(/disabled/, { timeout: 10000 });

      // Enable the users stream
      const streamCheckbox = usersStreamRow.locator('input[data-testid="sync-stream-checkbox"]');
      await streamCheckbox.check({ force: true, timeout: 10000 });
      await expect(streamCheckbox).toBeChecked({ timeout: 10000 });

      // Verify stream no longer has disabled style
      await expect(usersStreamRow).not.toHaveClass(/disabled/, { timeout: 10000 });

      // Verify stream doesn't have added style (should be default)
      await expect(usersStreamRow).not.toHaveClass(/added/, { timeout: 10000 });

      // Verify missing cursor error is displayed (since no sync mode is set)
      return expect(usersStreamRow.locator("text=Cursor missing")).toBeVisible({ timeout: 10000 });
    });

    test("should expand and collapse stream", async ({ page }) => {
      // Filter by stream name to find the users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Find and click expand/collapse button
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Verify stream is expanded
      return expect(expandButton).toHaveAttribute("aria-expanded", "true", { timeout: 10000 });
    });

    test("should enable field", async ({ page }) => {
      // Filter by stream name to find the users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Verify stream is initially disabled
      const streamCheckbox = usersStreamRow.locator('input[data-testid="sync-stream-checkbox"]');
      await expect(streamCheckbox).not.toBeChecked({ timeout: 10000 });

      // Expand the stream to see fields
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Find the email field row
      const emailFieldRow = page.locator('[data-testid="row-depth-2-field-email"]');
      await expect(emailFieldRow).toBeVisible({ timeout: 10000 });

      // Verify email field has disabled style initially
      await expect(emailFieldRow).toHaveClass(/disabled/, { timeout: 10000 });

      // Enable the email field
      const emailFieldCheckbox = emailFieldRow.locator('input[data-testid="sync-field-checkbox"]');
      await emailFieldCheckbox.check({ force: true, timeout: 10000 });

      // Verify email field no longer has disabled style
      await expect(emailFieldRow).not.toHaveClass(/disabled/, { timeout: 10000 });

      // Verify email field doesn't have added style
      await expect(emailFieldRow).not.toHaveClass(/added/, { timeout: 10000 });

      // Verify id field checkbox is disabled (should be primary key)
      const idFieldRow = page.locator('[data-testid="row-depth-2-field-id"]');
      const idFieldCheckbox = idFieldRow.locator('input[data-testid="sync-field-checkbox"]');
      await expect(idFieldCheckbox).toBeDisabled({ timeout: 10000 });

      // Verify id field is marked as primary key
      await expect(idFieldRow.locator('[data-testid="field-pk-cell"]')).toContainText("primary key", {
        timeout: 10000,
      });

      // Verify email field is now enabled
      await expect(emailFieldCheckbox).toBeChecked({ timeout: 10000 });

      // Verify missing cursor error is still displayed
      await expect(usersStreamRow.locator("text=Cursor missing")).toBeVisible({ timeout: 10000 });

      // Verify that enabling a field also enables the stream
      await expect(streamCheckbox).toBeChecked({ timeout: 10000 });

      // Verify namespace checkbox is in mixed state
      const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
      return expect(namespaceCheckbox).toHaveAttribute("aria-checked", "mixed", { timeout: 10000 });
    });

    test("should enable form submit after a stream is selected and configured", async ({ page }) => {
      // Filter by stream name to find the users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Enable the users stream first
      const streamCheckbox = usersStreamRow.locator('input[data-testid="sync-stream-checkbox"]');
      await streamCheckbox.check({ force: true, timeout: 10000 });

      // Expand the stream to access sync mode dropdown
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Wait for the sync mode button to be visible after expansion
      const syncModeButton = usersStreamRow.locator('button[data-testid="sync-mode-select-listbox-button"]');
      await expect(syncModeButton).toBeVisible({ timeout: 10000 });

      await selectSyncMode(page, usersStreamRow, "Full refresh | Overwrite");

      // Verify no streams selected error is not displayed
      await expect(page.locator("text=Select at least 1 stream to sync.")).not.toBeVisible({ timeout: 10000 });

      // Wait for next page button to appear, then verify it's enabled
      const nextButton = page.locator('[data-testid="next-creation-page"]');
      await expect(nextButton).toBeVisible({ timeout: 15000 });
      return expect(nextButton).toBeEnabled({ timeout: 10000 });
    });
  });

  test.describe("Submit form", () => {
    test("should set up a connection and redirect to connection overview page", async ({ page, airbyte }) => {
      // Navigate to the configuration page
      await openConfiguration(page, airbyte);

      // Filter by stream name to find users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Enable the users stream
      const streamCheckbox = usersStreamRow.locator('input[data-testid="sync-stream-checkbox"]');
      await streamCheckbox.check({ force: true, timeout: 10000 });

      // Expand the stream to configure sync mode
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Wait for the sync mode button to be visible after expansion
      const syncModeButton = usersStreamRow.locator('button[data-testid="sync-mode-select-listbox-button"]');
      await expect(syncModeButton).toBeVisible({ timeout: 10000 });

      // Configure sync mode using helper
      await selectSyncMode(page, usersStreamRow, "Full refresh | Overwrite");

      // Register the response listener before triggering the request, so we can't miss a fast reply
      const responsePromise = page.waitForResponse("**/api/v1/web_backend/connections/create", { timeout: 30000 });

      // Complete the connection creation flow with manual schedule to avoid kicking off the sync job on creation
      await completeConnectionCreation(page, { scheduleType: "Manual" });

      // Wait for the response and extract connection ID
      const response = await responsePromise;
      expect(response.status()).toBe(200);

      // Verify the request
      const createRequest = response.request();
      expect(createRequest.method()).toBe("POST");

      // Get the request body to verify connection details
      const requestBody = createRequest.postDataJSON();
      expect(requestBody.name).toBe(`${airbyte.source.name} → ${airbyte.destination.name}`);
      expect(requestBody.scheduleType).toBe("manual");

      const responseBody = await response.json();
      expect(responseBody.name).toBe(`${airbyte.source.name} → ${airbyte.destination.name}`);
      expect(responseBody.scheduleType).toBe("manual");

      const connectionId = responseBody.connectionId;
      expect(connectionId).toBeDefined();

      // Verify we're redirected to the connection overview page after creation
      expect(airbyte.creations).toHaveLength(1);
      expect(airbyte.creations[0]).toMatchObject({
        sourceId: airbyte.source.sourceId,
        destinationId: airbyte.destination.destinationId,
        syncCatalog: {
          streams: expect.arrayContaining([
            expect.objectContaining({
              stream: expect.objectContaining({ name: "users", namespace: "public" }),
              config: expect.objectContaining({
                selected: true,
                syncMode: "full_refresh",
                destinationSyncMode: "overwrite",
              }),
            }),
          ]),
        },
      });
      expect(airbyte.creations[0].syncCatalog?.streams.filter(({ config }) => config?.selected)).toHaveLength(1);

      await expect(page).toHaveURL(new RegExp(`.*/connections/${connectionId}/status`));
      await page.reload();
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "pending");
      expect(airbyte.connections[0].syncCatalog).toEqual(requestBody.syncCatalog);
    });
  });
});
