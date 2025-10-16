import { Page, expect, Request, Locator } from "@playwright/test";
import { SourceRead, DestinationRead, SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

// This file contains helper methods for connection creation UI interactions

/**
 * Sync mode value combining source and destination sync modes
 * Matches the SyncModeValue type used in the webapp
 */
export interface SyncModeValue {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
}

/**
 * Sync mode display strings that match the UI format
 */
export type SyncModeDisplayString = string;

/**
 * Common sync mode combinations for convenience
 * Matches the SUPPORTED_MODES in the webapp
 */
export const CommonSyncModes = {
  FULL_REFRESH_OVERWRITE: {
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.overwrite,
  } as SyncModeValue,
  FULL_REFRESH_APPEND: {
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.append,
  } as SyncModeValue,
  INCREMENTAL_APPEND: {
    syncMode: SyncMode.incremental,
    destinationSyncMode: DestinationSyncMode.append,
  } as SyncModeValue,
  INCREMENTAL_DEDUP: {
    syncMode: SyncMode.incremental,
    destinationSyncMode: DestinationSyncMode.append_dedup,
  } as SyncModeValue,
} as const;

/**
 * Navigate to connection configuration page and wait for streams table to load
 */
export const navigateToConnectionConfig = async (
  page: Page,
  workspaceId: string,
  source: SourceRead,
  destination: DestinationRead,
  options: {
    setupDiscoverSchemaIntercept?: boolean;
    timeout?: number;
  } = {}
): Promise<void> => {
  const { setupDiscoverSchemaIntercept = true, timeout = 20000 } = options;

  if (setupDiscoverSchemaIntercept) {
    // Set up API interceptor for discover schema request
    await page.route("**/sources/discover_schema", async (route) => {
      await route.continue();
    });
  }

  // Navigate directly to the configuration page with source and destination
  const configUrl = `/workspaces/${workspaceId}/connections/new-connection/configure?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`;
  await page.goto(configUrl, { timeout });

  // Wait for the streams table to load
  await page.waitForSelector('table[data-testid="sync-catalog-table"]', { timeout: 90000 });
  // Also wait for actual stream content to ensure the table is fully populated
  await page.waitForSelector('[data-testid^="row-depth-1-stream"]', { timeout: 15000 });
};

/**
 * Converts SyncModeValue to display string format
 * Matches the format used in the UI: "Full refresh | Overwrite"
 */
const formatSyncModeDisplayString = (syncMode: SyncMode, destinationSyncMode: DestinationSyncMode): string => {
  const syncModeStrings: Record<SyncMode, string> = {
    [SyncMode.full_refresh]: "Full refresh",
    [SyncMode.incremental]: "Incremental",
  };

  const destinationSyncModeStrings: Record<DestinationSyncMode, string> = {
    [DestinationSyncMode.append]: "Append",
    [DestinationSyncMode.append_dedup]: "Append + Deduped",
    [DestinationSyncMode.overwrite]: "Overwrite",
    [DestinationSyncMode.overwrite_dedup]: "Overwrite + Deduped",
    [DestinationSyncMode.update]: "Update",
    [DestinationSyncMode.soft_delete]: "Soft Delete",
  };

  return `${syncModeStrings[syncMode]} | ${destinationSyncModeStrings[destinationSyncMode]}`;
};

/**
 * Select sync mode for a stream
 * Supports both display string format and SyncModeValue object
 */
export const selectSyncMode = async (
  page: Page,
  streamRow: Locator,
  syncMode: SyncModeDisplayString | SyncModeValue = "Full refresh | Overwrite",
  options: {
    waitForCursorErrorToDisappear?: boolean;
    timeout?: number;
  } = {}
) => {
  const { waitForCursorErrorToDisappear = true, timeout = 10000 } = options;

  // Convert SyncModeValue to display string if needed
  const displayString =
    typeof syncMode === "string"
      ? syncMode
      : formatSyncModeDisplayString(syncMode.syncMode, syncMode.destinationSyncMode);

  // Find and click the sync mode button
  const syncModeButton = streamRow.locator('button[data-testid="sync-mode-select-listbox-button"]');
  await syncModeButton.click({ timeout });

  // Wait for sync mode options menu to appear
  const syncModeMenu = page.locator('ul[data-testid="sync-mode-select-listbox-options"]');
  await expect(syncModeMenu).toBeVisible({ timeout });

  // Select the specified sync mode option
  const syncModeOption = syncModeMenu.getByRole("option", { name: displayString, exact: true });
  await syncModeOption.click({ force: true, timeout });

  if (waitForCursorErrorToDisappear) {
    // Wait for sync mode to be applied and "Cursor missing" error to disappear
    await expect(streamRow.locator("text=Cursor missing")).not.toBeVisible({ timeout });
  }
};

/**
 * Click button using DOM evaluation to bypass overlays (ie React dev tools)
 */
export const clickButtonBypassingOverlay = async (
  page: Page,
  selector: string,
  errorMessage: string = `Button with selector ${selector} not found in DOM`
) => {
  await page.evaluate(
    (args) => {
      const button = document.querySelector(args.selector) as HTMLElement;
      if (button) {
        button.click();
      } else {
        throw new Error(args.errorMessage);
      }
    },
    { selector, errorMessage }
  );
};

/**
 * Set up API interceptor for connection creation requests
 * Returns the requests array for assertions
 */
export const setupConnectionCreationIntercept = async (page: Page): Promise<Request[]> => {
  const createConnectionRequests: Request[] = [];

  await page.route("**/api/v1/web_backend/connections/create", (route) => {
    createConnectionRequests.push(route.request());
    return route.continue();
  });

  return createConnectionRequests;
};

/**
 * Find a stream row by namespace and stream name
 */
export const findStreamRow = (page: Page, namespace: string, streamName: string) => {
  return page.locator(`[data-testid="row-depth-1-stream-${namespace}.${streamName}"]`);
};

/**
 * Filter streams by name and find a specific stream row
 */
export const filterAndFindStream = async (
  page: Page,
  streamName: string,
  options: { timeout?: number; waitForVisibility?: boolean } = {}
) => {
  const { timeout = 10000, waitForVisibility = true } = options;

  // Filter by stream name
  const searchInput = page.locator('input[data-testid="sync-catalog-search"]');
  await searchInput.fill(streamName, { timeout });

  // Find the stream row (handle both namespaced and non-namespaced cases)
  const streamRow = page.locator(`[data-testid="row-depth-1-stream-${streamName}"]`);

  if (waitForVisibility) {
    await expect(streamRow).toBeVisible({ timeout });
  }

  return streamRow;
};

/**
 * Enable/disable a stream by toggling its checkbox
 */
export const toggleStreamEnabled = async (
  page: Page,
  namespace: string,
  streamName: string,
  enabled: boolean,
  options: { timeout?: number } = {}
) => {
  const { timeout = 10000 } = options;
  const streamRow = findStreamRow(page, namespace, streamName);
  const checkbox = streamRow.locator('input[data-testid="sync-stream-checkbox"]');

  const isCurrentlyChecked = await checkbox.isChecked();
  if (isCurrentlyChecked !== enabled) {
    await checkbox.click({ timeout });
  }
};

/**
 * Expand/collapse a stream row
 */
export const toggleStreamExpanded = async (
  page: Page,
  namespace: string,
  streamName: string,
  options: { timeout?: number } = {}
) => {
  const { timeout = 10000 } = options;
  const streamRow = findStreamRow(page, namespace, streamName);
  const expandButton = streamRow.locator('[data-testid="expand-stream-btn"]');
  await expandButton.click({ timeout });
};

/**
 * Toggle field selection within a stream
 */
export const toggleFieldEnabled = async (
  page: Page,
  namespace: string,
  streamName: string,
  fieldName: string,
  enabled: boolean,
  options: { timeout?: number } = {}
) => {
  const { timeout = 10000 } = options;
  const streamRow = findStreamRow(page, namespace, streamName);
  const fieldRow = streamRow.locator(`[data-testid="row-depth-2-field-${fieldName}"]`);
  const fieldCheckbox = fieldRow.locator('input[data-testid="sync-field-checkbox"]');

  const isCurrentlyChecked = await fieldCheckbox.isChecked();
  if (isCurrentlyChecked !== enabled) {
    await fieldCheckbox.click({ timeout });
  }
};

/**
 * Complete connection creation flow - navigate to next step and submit
 */
export const completeConnectionCreation = async (
  page: Page,
  options: {
    nextButtonTimeout?: number;
    submitButtonTimeout?: number;
    useOverlayWorkaround?: boolean;
    scheduleType?: "Scheduled" | "Manual" | "Cron";
  } = {}
) => {
  const { nextButtonTimeout = 15000, submitButtonTimeout = 10000, useOverlayWorkaround = true, scheduleType } = options;

  // Wait for and click Next button
  const nextButton = page.locator('[data-testid="next-creation-page"]');
  await expect(nextButton).toBeVisible({ timeout: nextButtonTimeout });
  await expect(nextButton).toBeEnabled({ timeout: 10000 });

  if (useOverlayWorkaround) {
    await clickButtonBypassingOverlay(page, '[data-testid="next-creation-page"]', "Next button not found in DOM");
  } else {
    await nextButton.click({ timeout: 10000 });
  }

  // Set schedule type if specified (defaults to "Scheduled" in UI)
  if (scheduleType) {
    const scheduleButton = page.locator('[data-testid="schedule-type-listbox-button"]');
    await scheduleButton.waitFor({ state: "visible", timeout: 10000 });

    // Click to open the dropdown
    await scheduleButton.click({ timeout: 10000 });

    // Click the desired option
    const option = page.locator(`[data-testid="${scheduleType.toLowerCase()}-option"]`);
    await option.waitFor({ state: "visible", timeout: 5000 });
    await option.click({ timeout: 5000 });

    // Verify selection
    await expect(scheduleButton).toContainText(scheduleType, { timeout: 5000 });
  }

  // Wait for and click Submit button
  const submitButton = page.locator('button[type="submit"]');
  await expect(submitButton).toBeVisible({ timeout: submitButtonTimeout });

  if (useOverlayWorkaround) {
    await clickButtonBypassingOverlay(page, 'button[type="submit"]', "Submit button not found in DOM");
  } else {
    await submitButton.click({ timeout: 10000 });
  }
};

/**
 * Namespace-level operations
 */
export const namespaceHelpers = {
  /**
   * Toggle namespace checkbox to enable/disable all streams in namespace
   */
  toggleNamespaceCheckbox: async (
    page: Page,
    namespace: string,
    enabled: boolean,
    options: { timeout?: number } = {}
  ) => {
    const { timeout = 10000 } = options;
    const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');

    const isCurrentlyChecked = await namespaceCheckbox.isChecked();
    if (isCurrentlyChecked !== enabled) {
      await namespaceCheckbox.click({ timeout });
    }
  },

  /**
   * Verify namespace row elements are visible
   */
  verifyNamespaceRow: async (
    page: Page,
    namespace: string,
    expectedStreamCount: number | string | RegExp = /\d+ streams?/,
    options: { timeout?: number } = {}
  ) => {
    const { timeout = 10000 } = options;

    // Verify namespace name is displayed
    await expect(page.locator("thead tr th").filter({ hasText: namespace })).toBeVisible({ timeout });

    // Verify stream count is displayed
    if (typeof expectedStreamCount === "number") {
      await expect(
        page.locator(`text=${expectedStreamCount} stream${expectedStreamCount === 1 ? "" : "s"}`)
      ).toBeVisible({ timeout });
    } else {
      await expect(page.locator("thead tr th").filter({ hasText: expectedStreamCount })).toBeVisible({ timeout });
    }

    // Verify gear button for namespace modal is displayed
    await expect(page.locator('thead tr th button[data-testid="destination-namespace-modal-btn"]')).toBeVisible({
      timeout,
    });

    // Verify column headers are displayed
    await expect(page.locator("thead tr").filter({ hasText: "Sync mode" })).toBeVisible({ timeout });
    await expect(page.locator("thead tr").filter({ hasText: "Fields" })).toBeVisible({ timeout });
  },
};
