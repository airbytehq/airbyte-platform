import { test } from "@playwright/test";

import {
  assertAutoImportSchemaMatches,
  assertHasNumberOfSlices,
  assertMaxNumberOfPages,
  assertMaxNumberOfSlicesAndPages,
  assertMultiPageReadItems,
  assertSchemaMismatch,
  assertTestPanelContains,
  assertTestReadItems,
  assertUrlPath,
} from "./assertions";
import {
  AUTO_IMPORTED_SCHEMA,
  SCHEMA_WITH_MISMATCH,
  configureInvalidAuth,
  configureListPartitionRouter,
  configurePagination,
  configureSchemaLoader,
  configureStream,
  configureValidAuth,
  getBuilderConnectorName,
  initializeBuilderConnector,
  navigateToDetectedSchema,
  navigateToStreamSchema,
  testStream,
  updateUrlPath,
  waitForDraftToSave,
} from "./helpers";

// Test Cases //

// Initialize the builder project before each test
// Authentication is handled globally, this just sets up the builder connector
test.beforeEach(async ({ page }) => {
  await initializeBuilderConnector(page);
  await configureStream(page);
  await configureValidAuth(page);
});

test("basic read succeeds", async ({ page }) => {
  await testStream(page);
  await assertTestReadItems(page);
  await assertAutoImportSchemaMatches(page, AUTO_IMPORTED_SCHEMA);
});

test("read fails with invalid auth", async ({ page }) => {
  await configureInvalidAuth(page);
  await testStream(page);
  await assertTestPanelContains(page, '"Bad credentials"');
});

test("read fails on 404", async ({ page }) => {
  await updateUrlPath(page, "nonexistent-endpoint/");
  await testStream(page);
  await assertTestPanelContains(page, '"status": 404');
});

test("read detects schema mismatch", async ({ page }) => {
  await navigateToStreamSchema(page);
  await configureSchemaLoader(page, SCHEMA_WITH_MISMATCH);
  await testStream(page);
  await navigateToDetectedSchema(page);
  await assertSchemaMismatch(page);
});

test("read succeeds with pagination", async ({ page }) => {
  await configurePagination(page);
  await testStream(page);
  await assertMultiPageReadItems(page);
});

test("pagination stops at test page limit", async ({ page }) => {
  await configurePagination(page);
  await updateUrlPath(page, "items/exceeding-page-limit/");
  await testStream(page);
  await assertMaxNumberOfPages(page);
});

test("read with list partition router", async ({ page }) => {
  await updateUrlPath(page, "items/{{ stream_partition.item_id }}");
  await configureListPartitionRouter(page, 3);
  await testStream(page);
  await assertHasNumberOfSlices(page, 3);
});

test("partitioned requests stop at test slice limit", async ({ page }) => {
  await updateUrlPath(page, "items/{{ stream_partition.item_id }}");
  await configureListPartitionRouter(page, 10);
  await testStream(page);
  await assertHasNumberOfSlices(page, 5);
});

test("pagination and partitioned requests stop at both limits", async ({ page }) => {
  await updateUrlPath(page, "items/exceeding-page-limit/{{ stream_partition.item_id }}");
  await configurePagination(page);
  await configureListPartitionRouter(page, 10);
  await testStream(page);
  await assertMaxNumberOfSlicesAndPages(page);
});

test("can return to saved connector after exiting project", async ({ page }) => {
  // Save the name of the connector for later use - use the first one (menu bar)
  const name = await getBuilderConnectorName(page);
  await updateUrlPath(page, "items/{{ stream_slice.item_id }}");
  await configureListPartitionRouter(page, 10);
  await testStream(page);
  await waitForDraftToSave(page);
  await page.goto("/connector-builder", { timeout: 10000 });
  await page.locator(`[data-testid="edit-project-button-${name}"]`).click({ timeout: 10000 });
  await page.waitForSelector('input[data-field-path="manifest.streams.0.name"]', { state: "visible", timeout: 10000 });
  await assertUrlPath(page, "items/{{ stream_slice.item_id }}");
});
