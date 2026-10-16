import { Page, expect } from "@playwright/test";

// UI interaction helpers for shared use across test suites

/**
 * Fill a form field by selector and wait for it to be ready
 * Simple form field filling for standard inputs
 */
export const fillFormField = async (page: Page, selector: string, value: string) => {
  const field = page.locator(selector);
  await field.waitFor({ state: "visible", timeout: 10000 });
  await field.fill(value);
};

/**
 * Navigate to connector creation page (handles both button click and manual navigation)
 * Flexible navigation that works whether "new connector" button exists or not
 */
export const navigateToConnectorCreation = async (
  page: Page,
  connectorType: "source" | "destination",
  workspaceId: string
) => {
  const basePage = `/workspaces/${workspaceId}/${connectorType}`;
  const createPage = `${basePage}/new-${connectorType}`;

  await page.goto(basePage, { timeout: 10000 });

  // Check if new connector button exists, click it if so
  const newButton = page.locator(`button[data-id='new-${connectorType}']`);
  const buttonCount = await newButton.count();

  if (buttonCount > 0) {
    await newButton.click();
    await expect(page).toHaveURL(new RegExp(`.*${createPage}`), { timeout: 10000 });
  } else {
    // If no button, manually navigate to create page
    await page.goto(createPage, { timeout: 10000 });
    await expect(page).toHaveURL(new RegExp(`.*${createPage}`), { timeout: 10000 });
  }
};

/**
 * Search for and select a connector from the marketplace catalog
 */
export const selectConnectorFromMarketplace = async (page: Page, searchTerm: string, connectorDisplayName: string) => {
  // Wait for connector selection page to fully load
  await page.waitForSelector('input[placeholder*="Search"]', { timeout: 15000 });

  // Search for the connector
  const searchInput = page.locator("input[placeholder*='Search']").first();
  await searchInput.waitFor({ state: "visible", timeout: 10000 });
  await searchInput.fill(searchTerm);

  // Click "see more marketplace" button to access marketplace connectors
  const marketplaceButton = page.locator("button[data-testid='see-more-marketplace']");
  await marketplaceButton.waitFor({ state: "visible", timeout: 10000 });
  await marketplaceButton.click();

  // Select connector from the filtered marketplace results
  const connectorButton = page.locator("button").filter({ hasText: connectorDisplayName });
  await connectorButton.waitFor({ state: "visible", timeout: 10000 });
  await connectorButton.click();
};
