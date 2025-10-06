import { test, expect, Request } from "@playwright/test";
import { SourceRead, DestinationRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { FeatureItem } from "@src/core/services/features/types";

import { connectionAPI, connectionUI, connectionTestHelpers } from "../../helpers/connection";
import { setupWorkspaceForTests } from "../../helpers/workspace";
import { setFeatureFlags, setFeatureServiceFlags, injectFeatureFlagsAndStyle } from "../../support/e2e";

test.describe("Connection Mappings", () => {
  // Uses environment-aware Postgres setup following dummy API pattern:
  // - CI: Uses POSTGRES_TEST_HOST environment variable to connect to deployed postgres pods
  // - Local: Falls back to platform-specific docker networking (host.docker.internal/172.17.0.1)
  let workspaceId: string;
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  test.beforeAll(async ({ request }) => {
    workspaceId = await setupWorkspaceForTests();

    // Create Postgres source and destination once for all mappings tests
    // Uses the same Postgres setup as status tests - 'users' table already exists in postgres-test-data.sql
    const testResources = await connectionTestHelpers.setupPostgresConnectionTest(
      request,
      workspaceId,
      "Postgres source",
      "Postgres destination"
    );

    postgresSource = testResources.source;
    postgresDestination = testResources.destination;

    // Create connection once and reuse across tests (reduces schema discovery calls)
    connection = await connectionAPI.create(request, postgresSource, postgresDestination, {
      enableAllStreams: true,
    });
  });

  test.afterEach(async () => {
    // Clear feature flags to avoid pollution between tests
    setFeatureFlags({});
    setFeatureServiceFlags({});
  });

  test.afterAll(async ({ request }) => {
    // Clean up resources using helper
    await connectionTestHelpers.cleanupConnectionTest(request, {
      connectionId: connection?.connectionId,
      sourceId: postgresSource?.sourceId,
      destinationId: postgresDestination?.destinationId,
    });
  });

  test.describe("With experiment on but feature disabled", () => {
    test("Shows upsell page if Feature is false", async ({ page, context }) => {
      // Set feature flags
      setFeatureFlags({});
      setFeatureServiceFlags({ [FeatureItem.MappingsUI]: false }); // Disabled to show upsell

      // Create a fresh page and inject feature flags into it
      const newPage = await context.newPage();
      await injectFeatureFlagsAndStyle(newPage);

      await connectionUI.visit(newPage, connection, "mappings");

      // Wait for upsell content to appear (since MAPPINGS_UI: false)
      await newPage.waitForSelector('[data-testid*="upsell"], button:has-text("Unlock")', {
        timeout: 20000,
      });

      await expect(newPage).toHaveURL(/\/mappings$/, { timeout: 10000 });
      await expect(newPage.locator('[data-testid="mappings-upsell-empty-state"]')).toBeVisible({
        timeout: 10000,
      });
    });
  });

  test.describe("With experiment and feature both enabled", () => {
    test("Allows configuring a first mapping", async ({ page, context }) => {
      // Set feature flags
      setFeatureFlags({});
      setFeatureServiceFlags({ [FeatureItem.MappingsUI]: true }); // Enabled to show mapping config

      // Create a fresh page and inject feature flags into it
      const newPage = await context.newPage();
      await injectFeatureFlagsAndStyle(newPage);

      // Set up API interceptors for mappings workflow
      const validateMappersRequests: Request[] = [];
      const updateConnectionRequests: Request[] = [];

      // Intercept validate mappers requests
      await newPage.route("**/api/v1/web_backend/mappers/validate", async (route) => {
        validateMappersRequests.push(route.request());
        await route.continue();
      });

      // Intercept update connection requests
      await newPage.route("**/api/v1/web_backend/connections/update", async (route) => {
        updateConnectionRequests.push(route.request());
        await route.continue();
      });

      // Navigate to mappings page
      await connectionUI.visit(newPage, connection, "mappings");

      // Wait for mapping config content to appear (since MAPPINGS_UI: true)
      await newPage.waitForSelector('[data-testid*="stream"], [data-testid*="combobox"], [data-testid*="add-stream"]', {
        timeout: 20000,
      });

      // Click add stream dropdown and select users
      await newPage.locator('[data-testid="add-stream-for-mapping-combobox"]').click();
      await newPage.getByRole("option", { name: "users" }).click();

      // Wait for initial validation call to fetch available fields
      await expect.poll(() => validateMappersRequests.length).toBeGreaterThanOrEqual(1);

      // Allow for extra render cycle
      await newPage.waitForTimeout(1000);

      // Click on field selection dropdown (should be empty initially)
      const fieldDropdown = newPage.locator('input[placeholder="Select a field"]').first();
      await expect(fieldDropdown).toHaveValue("");
      await fieldDropdown.click();

      // Select "name" field
      await newPage.getByRole("option", { name: "name" }).click();

      // Wait for second validation call after field selection
      await expect.poll(() => validateMappersRequests.length).toBeGreaterThanOrEqual(2);

      // Submit the mappings configuration
      await newPage.locator('[data-testid="submit-mappings"]').click();

      // Handle either refresh modal or clear streams modal (CI vs local differences)
      const refreshModalSave = newPage.locator('[data-testid="refreshModal-save"]');
      const clearStreamsSubmit = newPage.locator('[data-testid="resetModal-save"]');

      // Wait for either modal to appear
      let isRefreshModal = false;
      try {
        // Try refresh modal first (more specific selector)
        await refreshModalSave.waitFor({ state: "visible", timeout: 15000 });
        isRefreshModal = true;
      } catch {
        // If refresh modal not found, try clear streams modal
        await clearStreamsSubmit.waitFor({ state: "visible", timeout: 15000 });

        // Clear streams modal requires selecting the first radio button
        // Use force click to bypass the wrapper div intercepting pointer events
        await newPage.locator('[data-testid="radio-button-tile-shouldClear-saveWithClear"]').click({ force: true });
      }

      // Click the appropriate submit button
      const modalButton = isRefreshModal ? refreshModalSave : clearStreamsSubmit;
      await modalButton.waitFor({ state: "visible", timeout: 15000 });
      await modalButton.click();

      // Wait for connection update request
      await expect.poll(() => updateConnectionRequests.length, { timeout: 10000 }).toBeGreaterThanOrEqual(1);

      // Verify success notification
      await expect(newPage.locator('[data-testid="notification-connection_settings_change_success"]')).toBeVisible({
        timeout: 15000,
      });
    });
  });
});
