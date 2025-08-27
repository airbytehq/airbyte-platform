import { Page, expect } from "@playwright/test";

import { navigateToStreamSchema } from "./helpers";

// Locate expected text in the test panel
export const assertTestPanelContains = async (page: Page, text: string) => {
  const escapedText = text.replace(/"/g, '\\"');
  await expect(page.locator(`pre:has-text("${escapedText}")`)).toBeVisible({ timeout: 15000 });
};

// Assert that the URL path matches the expected value
export const assertUrlPath = async (page: Page, expectedUrlPath: string) => {
  const urlPathField = page.locator('div[data-field-path="manifest.streams.0.retriever.requester.url"] textarea');
  const urlValue = await urlPathField.inputValue();
  expect(urlValue).toContain(expectedUrlPath);
};

// Assert that the basic read test succeeded with expected items
export const assertTestReadItems = async (page: Page) => {
  await assertTestPanelContains(page, '"name": "abc"');
  await assertTestPanelContains(page, '"name": "def"');
};

// Assert that multi-page read succeeded with expected items on both pages
export const assertMultiPageReadItems = async (page: Page) => {
  // Check first page
  await assertTestReadItems(page);
  // Check second page
  const test_pages = page.locator('[data-testid="test-pages"]');
  await test_pages.locator('a[aria-label="Page 2"]').click();
  await assertTestPanelContains(page, '"name": "xxx"');
  await assertTestPanelContains(page, '"name": "yyy"');
};

// Assert that the auto-imported schema matches the expected schema
export const assertAutoImportSchemaMatches = async (page: Page, schema: string) => {
  await navigateToStreamSchema(page);
  const autoImportedSchemaText = await page.locator('pre[data-testid="auto-import-schema-json"]').textContent();
  expect(autoImportedSchemaText).toEqual(schema);
};

// Assert that schema mismatch is detected and shown
export const assertSchemaMismatch = async (page: Page) => {
  await expect(page.locator('[data-testid="schema-conflict-message"]')).toBeVisible();
};

// Assert that pagination is limited to maximum number of pages
export const assertMaxNumberOfPages = async (page: Page) => {
  const MAX_NUMBER_OF_PAGES = 5;
  const GO_BACK_AND_GO_NEXT_BUTTONS = 2;

  // Check that all expected page numbers exist
  for (let i = 1; i <= MAX_NUMBER_OF_PAGES; i++) {
    await expect(page.locator(`[data-testid="test-pages"] li:has-text("${i}")`)).toBeVisible();
  }

  // Check that the total number of page elements is limited (pages + navigation buttons)
  const pageElements = page.locator('[data-testid="test-pages"] li');
  await expect(pageElements).toHaveCount(MAX_NUMBER_OF_PAGES + GO_BACK_AND_GO_NEXT_BUTTONS);
};

// Assert that the correct number of slices are created
export const assertHasNumberOfSlices = async (page: Page, numberOfSlices: number) => {
  // Click on the slice dropdown to open it
  await page.locator('[data-testid="tag-select-slice-listbox-button"]').click();

  // Get all slice options and verify the count
  const sliceOptions = page.locator('[data-testid="tag-select-slice-listbox-options"] li');
  await expect(sliceOptions).toHaveCount(numberOfSlices);
};

// Assert that the correct number of slices and pages are created
export const assertMaxNumberOfSlicesAndPages = async (page: Page) => {
  const MAX_NUMBER_OF_SLICES = 5;

  // For each slice, check that it has the maximum number of pages
  for (let i = 0; i < MAX_NUMBER_OF_SLICES; i++) {
    // Click on the slice dropdown to open it
    await page.locator('[data-testid="tag-select-slice-listbox-button"]').click();

    // Click on the specific slice (Partition 1, Partition 2, etc.)
    await page.locator(`[data-testid="tag-select-slice-listbox-options"] li:has-text("Partition ${i + 1}")`).click();

    // Assert that this slice has the maximum number of pages
    await assertMaxNumberOfPages(page);
  }
};
