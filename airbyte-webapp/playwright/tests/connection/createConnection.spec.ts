import { test, expect, Request } from "@playwright/test";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { connectionTestHelpers } from "../../helpers/connection";
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
    // Uses the same Postgres setup as status/mappings tests
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
        await page.locator(sourceSelector).click({ timeout: 10000 });

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
});
