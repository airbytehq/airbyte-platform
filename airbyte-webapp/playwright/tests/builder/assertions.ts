import { Page } from "@playwright/test";

import { navigateToStreamSchema } from "./helpers";
import { expect } from "../../mock";

// Each check uses expect.soft, so one failed check does not hide the next ones. Some checks first open a tab or a
// dropdown, and close the dropdown again.

/** Escapes the characters that have a special meaning in a regular expression, such as "." and "{". */
const escapeRegExp = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Locate expected text in the test panel
export const assertTestPanelContains = async (page: Page, text: string) => {
  const escapedText = text.replace(/"/g, '\\"');
  await expect.soft(page.locator(`pre:has-text("${escapedText}")`)).toBeVisible();
};

// Assert that the URL of the stream ends with the expected path
export const assertUrlPath = async (page: Page, expectedUrlPath: string) => {
  const urlPathField = page.locator('div[data-field-path="manifest.streams.0.retriever.requester.url"] textarea');
  await expect.soft(urlPathField).toHaveValue(new RegExp(`${escapeRegExp(expectedUrlPath)}$`));
};

// Assert that the auto-imported schema matches the expected schema, including its whitespace
export const assertAutoImportSchemaMatches = async (page: Page, schema: string) => {
  await navigateToStreamSchema(page);
  const schemaText = page.locator('pre[data-testid="auto-import-schema-json"]');
  // toHaveText waits for the schema, but it ignores differences in whitespace. The second check compares the exact text.
  await expect.soft(schemaText).toHaveText(schema);
  expect.soft(await schemaText.textContent()).toBe(schema);
};

// Assert that schema mismatch is detected and shown
export const assertSchemaMismatch = async (page: Page) => {
  await expect.soft(page.locator('[data-testid="schema-conflict-message"]')).toBeVisible();
};

// Assert that the test panel offers the given number of pages
export const assertNumberOfPages = async (page: Page, numberOfPages: number) => {
  const GO_BACK_AND_GO_NEXT_BUTTONS = 2;

  for (let i = 1; i <= numberOfPages; i++) {
    await expect.soft(page.locator(`[data-testid="test-pages"] li:has-text("${i}")`)).toBeVisible();
  }
  await expect
    .soft(page.locator('[data-testid="test-pages"] li'))
    .toHaveCount(numberOfPages + GO_BACK_AND_GO_NEXT_BUTTONS);
};

// Assert that the slice dropdown offers the given number of slices. The check opens the dropdown and closes it again.
export const assertHasNumberOfSlices = async (page: Page, numberOfSlices: number) => {
  await page.locator('[data-testid="tag-select-slice-listbox-button"]').click();
  await expect.soft(page.locator('[data-testid="tag-select-slice-listbox-options"] li')).toHaveCount(numberOfSlices);
  await page.keyboard.press("Escape");
};

// Assert that each slice offers the given number of pages
export const assertNumberOfPagesInEachSlice = async (page: Page, numberOfSlices: number, numberOfPages: number) => {
  for (let i = 0; i < numberOfSlices; i++) {
    await page.locator('[data-testid="tag-select-slice-listbox-button"]').click();
    await page.locator(`[data-testid="tag-select-slice-listbox-options"] li:has-text("Partition ${i + 1}")`).click();
    await assertNumberOfPages(page, numberOfPages);
  }
};

// Assert that the test panel says that the read stopped at a limit of the test read
export const assertTestReadLimitReached = async (page: Page) => {
  await expect.soft(page.getByText("Stream testing limit reached.")).toBeVisible();
};
