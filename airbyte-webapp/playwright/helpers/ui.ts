import { Page, expect } from "@playwright/test";

// UI interaction helpers for shared use across test suites

/**
 * Generate unique form input values by appending random string
 * Helpful for creating unique connector names, avoiding conflicts in tests
 */
export const appendRandomString = (string: string) => {
  const randomString = Math.random().toString(36).substring(2, 10);
  return `${string} _${randomString}`;
};

/**
 * Click a submit button with consistent timeout handling
 */
export const submitButtonClick = async (page: Page) => {
  await page.locator("button[type=submit]").click({ timeout: 10000 });
};

/**
 * Select a dropdown option within a field container (Builder-style dropdowns)
 * Used for complex form dropdowns that require clicking initial option then target
 */
export const selectDropdownOption = async (
  page: Page,
  fieldPath: string,
  initialOption: string,
  targetOption: string
) => {
  await page.locator(`div[data-field-path="${fieldPath}"]`).locator(`span:text-is("${initialOption}")`).click();
  await page.locator(`p:text-is("${targetOption}")`).click();
};

/**
 * Fill input fields by label - handles both standard inputs and Monaco Editor
 * Advanced form filling that works with complex nested input structures
 */
export const fillInputByLabel = async (page: Page, fieldPath: string, value: string) => {
  const label = page.locator(`label[for="${fieldPath}"]`);
  await label.waitFor({ state: "visible", timeout: 10000 });
  if ((await label.count()) > 0) {
    // First try to find a standard input container
    const inputContainer = label.locator('xpath=../..//div[@data-testid="input-container"]');
    const standardInput = inputContainer.locator('[data-testid="input"]');

    if ((await standardInput.count()) > 0) {
      // Standard HTML input found
      await standardInput.waitFor({ state: "visible", timeout: 5000 });
      await standardInput.fill(value);
    }

    // If no standard input, look for Monaco Editor (JinjaInput)
    const monacoEditor = label.locator("xpath=../..//section").locator(".monaco-editor textarea");
    if ((await monacoEditor.count()) > 0) {
      // Monaco Editor found - focus and type into it
      await monacoEditor.waitFor({ state: "visible", timeout: 5000 });
      await monacoEditor.click();
      await monacoEditor.fill(value);
    }
  }
};

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
 * Submit form and wait for creation with URL validation
 * Specialized for connector creation workflows
 */
export const submitFormAndWaitForCreation = async (
  page: Page,
  connectorType: "source" | "destination",
  timeoutMs = 30000
) => {
  const submitButton = page.locator("button[type='submit']");
  await submitButton.waitFor({ state: "visible", timeout: 10000 });
  await submitButton.click();

  // Wait for successful creation (URL should include connector UUID)
  await expect(page).toHaveURL(new RegExp(`.*/${connectorType}/[a-f0-9-]{36}`), { timeout: timeoutMs });

  // Extract the connector ID from the URL for cleanup tracking
  const currentUrl = page.url();
  const idMatch = currentUrl.match(new RegExp(`/${connectorType}/([a-f0-9-]{36})`));
  if (idMatch) {
    return idMatch[1]; // Return the connector ID
  }
  throw new Error(`Could not extract ${connectorType} UUID from URL for cleanup`);
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

/**
 * Verify that a form update was successful
 * Checks for success indicator and expected field value
 */
export const verifyUpdateSuccess = async (page: Page, expectedValue: string) => {
  await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 15000 });
  await expect(page.locator(`input[value='${expectedValue}']`)).toBeVisible({ timeout: 10000 });
};

/**
 * Perform a second edit workflow on a text input field
 * Used to verify form remains functional after first successful update
 */
export const performSecondTextEdit = async (page: Page, fieldName: string, newValue: string) => {
  // Wait for success message to fade/reset (indicating form is ready for next edit)
  await page.waitForTimeout(2000);

  // Make another change to verify form is still functional
  const fieldInput = page.locator(`input[name='${fieldName}']`);
  await fieldInput.waitFor({ state: "visible", timeout: 10000 });
  await fieldInput.clear();
  await fieldInput.fill(newValue);

  // Verify button becomes enabled and submit the second change
  await expect(page.locator("button[type=submit]")).toBeEnabled({ timeout: 10000 });
  const submitButton = page.locator("button[type='submit']");
  await submitButton.waitFor({ state: "visible", timeout: 10000 });
  await submitButton.click();

  await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 60000 });
  await expect(page.locator(`input[value='${newValue}']`)).toBeVisible({ timeout: 10000 });
};

/**
 * Perform a second edit workflow on a dropdown field
 * Used to verify form remains functional after first successful update
 */
export const performSecondDropdownEdit = async (page: Page, fieldTestId: string, newValue: string) => {
  // Wait for success message to fade/reset (indicating form is ready for next edit)
  await page.waitForTimeout(2000);

  // Make another change to verify form is still functional
  const dropdown = page.locator(`[data-testid='${fieldTestId}']`);
  await dropdown.waitFor({ state: "visible", timeout: 10000 });
  await dropdown.click();

  // Select the new value from dropdown
  const option = page.locator("li").filter({ hasText: newValue });
  await option.waitFor({ state: "visible", timeout: 10000 });
  await option.click();

  // Verify button becomes enabled and submit the second change
  await expect(page.locator("button[type=submit]")).toBeEnabled({ timeout: 10000 });
  const submitButton = page.locator("button[type='submit']");
  await submitButton.waitFor({ state: "visible", timeout: 10000 });
  await submitButton.click();

  await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 30000 });
  await expect(page.locator(`[data-testid='${fieldTestId}']`)).toContainText(newValue, { timeout: 10000 });
};
