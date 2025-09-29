import { test, expect } from "@playwright/test";

import { pokeSourceAPI, sourceUI } from "../../helpers/connectors";
import { mockHelpers } from "../../helpers/mocks";
import {
  appendRandomString,
  navigateToConnectorCreation,
  selectConnectorFromMarketplace,
  fillFormField,
  performSecondDropdownEdit,
} from "../../helpers/ui";
import { setupWorkspaceForTests } from "../../helpers/workspace";

test.describe("Source CRUD operations", () => {
  let workspaceId: string;
  const createdSourceIds: string[] = [];

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test.afterAll(async ({ request }) => {
    // Clean up only sources created by this worker
    if (createdSourceIds.length > 0) {
      // Small delay to allow trace/screenshot generation to complete
      await new Promise((resolve) => setTimeout(resolve, 1000));

      for (const sourceId of createdSourceIds) {
        try {
          await pokeSourceAPI.delete(request, sourceId);
        } catch (error) {
          // Just warn on cleanup errors to avoid test failures
          console.warn(`⚠️ Failed to clean up source ${sourceId}:`, error);
        }
      }
    }
  });

  test("Redirects to connector selection when no sources exist", async ({ page }) => {
    await mockHelpers.mockEmptyConnectorLists(page, "source");

    await page.goto(`/workspaces/${workspaceId}/source`, { timeout: 15000 });

    await expect(page).toHaveURL(/.*\/workspaces\/[^/]+\/source\/new-source/, { timeout: 15000 });
    await expect(page.locator("h2")).toContainText("Set up a new source", { timeout: 20000 });
  });

  test("Can create new source", async ({ page }) => {
    const sourceName = appendRandomString("PokeAPI source");

    // Create source via UI
    const sourceId = await sourceUI.createViaUI(page, sourceName, workspaceId);
    createdSourceIds.push(sourceId); // Track for cleanup

    // Verify we're on the source detail page
    await expect(page).toHaveURL(/.*\/workspaces\/[^/]+\/source\/[a-f0-9-]{36}/, { timeout: 15000 });
  });

  test("Can update configured source", async ({ page, request }) => {
    const sourceName = appendRandomString("PokeAPI Source");
    const source = await pokeSourceAPI.create(request, sourceName, workspaceId);
    createdSourceIds.push(source.sourceId); // Track for cleanup

    await sourceUI.updateSource(page, sourceName, "connectionConfiguration.pokemon_name", "ivysaur", workspaceId);

    await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 15000 });
    await expect(page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']")).toContainText(
      "ivysaur",
      { timeout: 10000 }
    );
  });

  test("Can edit source again without leaving the page", async ({ page, request }) => {
    const sourceName = appendRandomString("PokeAPI Source");
    const source = await pokeSourceAPI.create(request, sourceName, workspaceId);
    createdSourceIds.push(source.sourceId); // Track for cleanup

    await sourceUI.updateSource(page, sourceName, "connectionConfiguration.pokemon_name", "ivysaur", workspaceId);

    await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 15000 });
    await expect(page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']")).toContainText(
      "ivysaur",
      { timeout: 10000 }
    );
    await expect(page.locator("button[type=submit]")).toBeDisabled({ timeout: 10000 });

    // Perform second edit to verify form is still functional
    await performSecondDropdownEdit(page, "connectionConfiguration.pokemon_name-listbox-button", "bulbasaur");
  });

  test("Can delete configured source", async ({ page, request }) => {
    const sourceName = appendRandomString("PokeAPI Source");
    const source = await pokeSourceAPI.create(request, sourceName, workspaceId);
    createdSourceIds.push(source.sourceId); // Track for cleanup initially

    // Delete via UI
    await sourceUI.deleteSource(page, sourceName, workspaceId);

    // Remove from cleanup list since we deleted it via UI
    const index = createdSourceIds.indexOf(source.sourceId);
    if (index > -1) {
      createdSourceIds.splice(index, 1);
    }

    // After deletion, should redirect to either list page or new-source page
    await expect(page).toHaveURL(/.*\/workspaces\/[^/]+\/source(\/new-source)?$/, { timeout: 15000 });

    // Verify deletion was successful via API (works regardless of UI state)
    const remainingSources = await pokeSourceAPI.list(request, workspaceId);
    const stillExists = remainingSources.some(
      (s: { name: string; sourceId: string }) => s.sourceId === source.sourceId
    );
    expect(stillExists).toBe(false);
  });
});

test.describe("Unsaved changes modal on create source page", () => {
  let workspaceId: string;

  test.beforeAll(async () => {
    workspaceId = await setupWorkspaceForTests();
  });

  test("Check leaving Source page without any changes", async ({ page }) => {
    // Setup required mocks
    await mockHelpers.mockEnterpriseConnectors(page);

    // Navigate to source creation page
    await navigateToConnectorCreation(page, "source", workspaceId);

    // Navigate away without making changes
    await page.goto(`/workspaces/${workspaceId}/connections`, { timeout: 10000 });

    // Should redirect successfully without modal
    await expect(page).toHaveURL(/.*\/workspaces\/[^/]+\/connections/, { timeout: 15000 });
    await expect(page.locator("[data-testid='confirmationModal']")).not.toBeVisible();
  });

  test("Check leaving Source page after failing testing", async ({ page }) => {
    // Setup required mocks including failed connection check
    await mockHelpers.mockEnterpriseConnectors(page);
    await mockHelpers.mockFailedConnectionCheck(page, "source");

    // Navigate to source creation page
    await navigateToConnectorCreation(page, "source", workspaceId);

    // Search for and select PokeAPI connector
    await selectConnectorFromMarketplace(page, "poke", "PokeAPI");

    // Fill in the form (this creates unsaved changes)
    await fillFormField(page, "input[name='name']", "testName");

    // Fill in the pokemon name (PokeAPI specific config)
    const pokemonDropdown = page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']");
    await pokemonDropdown.waitFor({ state: "visible", timeout: 10000 });
    await pokemonDropdown.click();

    // Select bulbasaur from the dropdown options
    const bulbasaurOption = page.locator("li").filter({ hasText: "bulbasaur" });
    await bulbasaurOption.waitFor({ state: "visible", timeout: 10000 });
    await bulbasaurOption.click();

    // Submit the form (will fail due to mocked failure)
    const submitButton = page.locator("button[type='submit']");
    await submitButton.waitFor({ state: "visible", timeout: 10000 });
    await submitButton.click();

    // Wait for the failed connection check to complete (small delay)
    await page.waitForTimeout(2000);

    // Try to navigate away by clicking on a navigation link (simulates real user behavior)
    // Look for the connections navigation link in the sidebar
    const connectionsNavLink = page
      .locator("nav")
      .getByRole("link", { name: /connections/i })
      .first();
    await connectionsNavLink.waitFor({ state: "visible", timeout: 10000 });
    await connectionsNavLink.click();

    // Should show confirmation modal - check existence in DOM (like Cypress approach)
    const modal = page.locator("[data-testid='confirmationModal']");
    await modal.waitFor({ state: "attached", timeout: 15000 });

    // Check modal content (following Cypress pattern)
    await expect(modal).toContainText("Unsaved changes will be lost", { timeout: 10000 });
    await expect(modal).toContainText(
      "Your changes will be lost if you navigate away from this page. Are you sure you want to leave?",
      { timeout: 10000 }
    );
  });
});
