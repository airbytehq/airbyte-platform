import { test, expect, Request } from "@playwright/test";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { connectionTestHelpers } from "../../helpers/connection";
import {
  navigateToConnectionConfig,
  selectSyncMode,
  setupConnectionCreationIntercept,
  filterAndFindStream,
  completeConnectionCreation,
  namespaceHelpers,
} from "../../helpers/connectionCreation";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Connection - Create new connection", () => {
  // - CI: Uses POSTGRES_TEST_HOST environment variable to connect to deployed postgres pods
  // - Local: Falls back to platform-specific docker networking (host.docker.internal/172.17.0.1)
  let workspaceId: string;
  let source: SourceRead;
  let destination: DestinationRead;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.beforeEach(async ({ page, request }) => {
    // Create Postgres source and destination for the connection creation tests
    const testResources = await connectionTestHelpers.setupPostgresConnectionTest(
      request,
      workspaceId,
      "Test source",
      "Test destination"
    );

    source = testResources.source;
    destination = testResources.destination;
  });

  test.afterEach(async ({ request }) => {
    await connectionTestHelpers.cleanupConnectionTest(request, {
      sourceId: source?.sourceId,
      destinationId: destination?.destinationId,
    });
  });

  test.describe("Set up connection", () => {
    test.describe("From connection page", () => {
      test("should open 'New connection' page", async ({ page }) => {
        // Set up API interceptors to track the requests made when opening new connection page
        const sourcesListRequests: Request[] = [];
        const sourceDefinitionsRequests: Request[] = [];

        // Intercept sources list request
        await page.route("**/api/v1/sources/list", async (route) => {
          sourcesListRequests.push(route.request());
          await route.continue();
        });

        // Intercept source definitions request
        await page.route("**/api/v1/source_definitions/list_for_workspace", async (route) => {
          sourceDefinitionsRequests.push(route.request());
          await route.continue();
        });

        // Navigate to connections list page
        await page.goto(`/workspaces/${workspaceId}/connections`, { timeout: 20000 });

        // Click new connection button to open the creation flow
        await page.locator('[data-testid="new-connection-button"]').click({ timeout: 10000 });

        // Verify that the required API calls were made
        await expect.poll(() => sourcesListRequests.length).toBeGreaterThanOrEqual(1);
        await expect.poll(() => sourceDefinitionsRequests.length).toBeGreaterThanOrEqual(1);

        // Verify we're on the new connection page
        await expect(page).toHaveURL(/\/connections\/new-connection$/, { timeout: 20000 });
      });

      test("should select existing Source", async ({ page }) => {
        // Navigate to the new connection page (continuing from previous test flow)
        await page.goto(`/workspaces/${workspaceId}/connections/new-connection`, { timeout: 20000 });

        // Verify that "existing connector" type is selected for source by default
        await expect(page.locator('input[data-testid="radio-button-tile-sourceType-existing"]')).toBeChecked({
          timeout: 10000,
        });
        await expect(page.locator('input[data-testid="radio-button-tile-sourceType-new"]')).not.toBeChecked({
          timeout: 10000,
        });

        // Select the source we created from the list
        const sourceSelector = `button[data-testid="select-existing-source-${source.name}"]`;
        await page.locator(sourceSelector).click({ timeout: 10000 });

        // Verify that selecting the source progressed us to destination selection
        // (The source section should now show as completed/selected)
        await expect(page.locator('input[data-testid="radio-button-tile-destinationType-existing"]')).toBeVisible({
          timeout: 10000,
        });
      });

      test("should select existing Destination", async ({ page }) => {
        // Navigate to the new connection page (continuing from previous test flow)
        await page.goto(`/workspaces/${workspaceId}/connections/new-connection`, { timeout: 20000 });

        // First select source to get to destination step
        const sourceSelector = `button[data-testid="select-existing-source-${source.name}"]`;
        await page.locator(sourceSelector).click({ timeout: 20000 });

        // Wait for destination selection to be visible
        await expect(page.locator('input[data-testid="radio-button-tile-destinationType-existing"]')).toBeVisible({
          timeout: 10000,
        });

        // Set up API interceptor for discover schema request
        const discoverSchemaRequests: Request[] = [];
        await page.route("**/sources/discover_schema", async (route) => {
          discoverSchemaRequests.push(route.request());
          await route.continue();
        });

        // Verify that "existing connector" type is selected for destination by default
        await expect(page.locator('input[data-testid="radio-button-tile-destinationType-existing"]')).toBeChecked();
        await expect(page.locator('input[data-testid="radio-button-tile-destinationType-new"]')).not.toBeChecked();

        // Select the destination we created from the list
        const destinationSelector = `button[data-testid="select-existing-destination-${destination.name}"]`;
        await page.locator(destinationSelector).click({ timeout: 10000 });

        // Wait for discover schema request to be made
        await expect.poll(() => discoverSchemaRequests.length).toBeGreaterThanOrEqual(1);
      });

      test("should redirect to 'New connection' configuration page with stream table", async ({ page }) => {
        // Navigate through the full flow: source -> destination -> configuration
        await page.goto(`/workspaces/${workspaceId}/connections/new-connection`, { timeout: 20000 });

        // Select source
        const sourceSelector = `button[data-testid="select-existing-source-${source.name}"]`;
        await page.locator(sourceSelector).click({ timeout: 10000 });

        // Wait for destination selection
        await expect(page.locator('input[data-testid="radio-button-tile-destinationType-existing"]')).toBeVisible({
          timeout: 10000,
        });

        // Select destination
        const destinationSelector = `button[data-testid="select-existing-destination-${destination.name}"]`;
        await page.locator(destinationSelector).click({ timeout: 10000 });

        // Verify we're redirected to the configuration page with stream table
        await expect(page).toHaveURL(/\/connections\/new-connection\/configure/, { timeout: 15000 });
      });
    });
  });

  test.describe("Streams table", () => {
    test.beforeEach(async ({ page }) => {
      await navigateToConnectionConfig(page, workspaceId, source, destination);
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
      await namespaceHelpers.verifyNamespaceRow(page, "public");
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
      await expect(page.locator('[data-testid="next-creation-page"]')).toBeDisabled({ timeout: 10000 });
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
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);
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
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);
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
    test.beforeEach(async ({ page }) => {
      await navigateToConnectionConfig(page, workspaceId, source, destination);
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
      await expect(usersStreamRow.locator("text=Cursor missing")).toBeVisible({ timeout: 10000 });

      // Disable the stream again
      await streamCheckbox.uncheck({ force: true, timeout: 10000 });
      await expect(streamCheckbox).not.toBeChecked({ timeout: 10000 });
    });

    test("should expand and collapse stream", async ({ page }) => {
      // Filter by stream name to find the users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Find and click expand/collapse button
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Verify stream is expanded
      await expect(expandButton).toHaveAttribute("aria-expanded", "true", { timeout: 10000 });

      // Click again to collapse
      await expandButton.click({ timeout: 10000 });

      // Verify stream is collapsed
      await expect(expandButton).toHaveAttribute("aria-expanded", "false", { timeout: 10000 });
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
      await expect(namespaceCheckbox).toHaveAttribute("aria-checked", "mixed", { timeout: 10000 });
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

      // Select sync mode using helper (following Cypress pattern exactly)
      // Add 1 second delay like Cypress to fix flaky behavior
      await page.waitForTimeout(1000);

      await selectSyncMode(page, usersStreamRow, "Full refresh | Overwrite");

      // Verify no streams selected error is not displayed
      await expect(page.locator("text=Select at least 1 stream to sync.")).not.toBeVisible({ timeout: 10000 });

      // Wait for next page button to appear, then verify it's enabled
      const nextButton = page.locator('[data-testid="next-creation-page"]');
      await expect(nextButton).toBeVisible({ timeout: 15000 });
      await expect(nextButton).toBeEnabled({ timeout: 10000 });
    });
  });

  test.describe("Submit form", () => {
    let connectionId: string;

    test("should set up a connection and redirect to connection overview page", async ({ page, request }) => {
      // Set up API interceptor for connection creation
      const createConnectionRequests = await setupConnectionCreationIntercept(page);

      // Navigate to the configuration page
      await navigateToConnectionConfig(page, workspaceId, source, destination);

      // Filter by stream name to find users stream
      const usersStreamRow = await filterAndFindStream(page, "users");

      // Enable the users stream
      const streamCheckbox = usersStreamRow.locator('input[data-testid="sync-stream-checkbox"]');
      await streamCheckbox.check({ force: true, timeout: 10000 });

      // Expand the stream to configure sync mode
      const expandButton = usersStreamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click({ timeout: 10000 });

      // Add 1 second delay like Cypress to fix flaky behavior
      await page.waitForTimeout(1000);

      // Configure sync mode using helper
      await selectSyncMode(page, usersStreamRow, "Full refresh | Overwrite");

      // Complete the connection creation flow
      await completeConnectionCreation(page);

      // Wait for connection creation request to be made
      await expect.poll(() => createConnectionRequests.length).toBeGreaterThanOrEqual(1);

      // Verify the request
      const createRequest = createConnectionRequests[0];
      expect(createRequest.method()).toBe("POST");

      // Get the request body to verify connection details
      const requestBody = await createRequest.postDataJSON();
      expect(requestBody.name).toBe(`${source.name} → ${destination.name}`);
      expect(requestBody.scheduleType).toBe("basic");

      // Wait for the response and extract connection ID
      const response = await page.waitForResponse("**/api/v1/web_backend/connections/create", { timeout: 30000 });
      expect(response.status()).toBe(200);

      const responseBody = await response.json();
      expect(responseBody.name).toBe(`${source.name} → ${destination.name}`);
      expect(responseBody.scheduleType).toBe("basic");

      connectionId = responseBody.connectionId;
      expect(connectionId).toBeDefined();

      // Verify we're redirected to the connection overview page after creation
      await expect(page).toHaveURL(new RegExp(`.*/connections/${connectionId}/status`), { timeout: 20000 });
    });

    test.afterAll(async ({ request }) => {
      // Clean up the created connection if it exists
      if (connectionId) {
        try {
          await connectionTestHelpers.cleanupConnectionTest(request, {
            connectionId,
          });
        } catch (error) {
          console.warn(`⚠️ Failed to clean up connection ${connectionId}:`, error);
        }
      }
    });
  });
});
