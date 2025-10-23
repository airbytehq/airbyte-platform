/**
 * Replication page helpers for Playwright tests
 * Handles schema change detection, catalog diff modal, and stream table interactions
 */

import { Page, expect } from "@playwright/test";
import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

// Sync mode display strings
const SYNC_MODE_STRINGS: Readonly<Record<SyncMode | DestinationSyncMode, string>> = {
  [SyncMode.full_refresh]: "Full refresh",
  [SyncMode.incremental]: "Incremental",
  [DestinationSyncMode.append]: "Append",
  [DestinationSyncMode.append_dedup]: "Append + Deduped",
  [DestinationSyncMode.overwrite]: "Overwrite",
  [DestinationSyncMode.overwrite_dedup]: "Overwrite + Deduped",
  [DestinationSyncMode.soft_delete]: "Soft Delete",
  [DestinationSyncMode.update]: "Update",
};

/**
 * Schema change detection banner helpers
 */
export const schemaChange = {
  /**
   * Verifies the schema change banner is visible with correct styling
   * @param breaking - true for error (breaking), false for info (non-breaking)
   */
  verifySchemaChangeBanner: async (page: Page, breaking: boolean): Promise<void> => {
    const banner = page.locator('[data-testid="schemaChangesDetected"]');
    await expect(banner).toBeVisible();

    // Check banner styling - breaking changes have error class
    const bannerClass = await banner.getAttribute("class");
    if (breaking) {
      expect(bannerClass).toMatch(/error/);
    } else {
      expect(bannerClass).toMatch(/info/);
    }

    // Breaking changes show a backdrop overlay
    const backdrop = page.locator('[data-testid="schemaChangesBackdrop"]');
    if (breaking) {
      await expect(backdrop).toBeVisible();
    } else {
      await expect(backdrop).not.toBeVisible();
    }
  },

  /**
   * Verifies the schema change banner is not visible
   */
  verifySchemaChangeBannerCleared: async (page: Page): Promise<void> => {
    await expect(page.locator('[data-testid="schemaChangesDetected"]')).not.toBeVisible();
    return expect(page.locator('[data-testid="schemaChangesBackdrop"]')).not.toBeVisible();
  },

  /**
   * Clicks the "Review changes" button on the schema change banner
   */
  clickReviewChangesButton: async (page: Page): Promise<void> => {
    const reviewButton = page.locator('[data-testid="schemaChangesDetected-button"]');
    await reviewButton.click();
    // Button should disappear after clicking
    return expect(reviewButton).not.toBeVisible();
  },

  /**
   * Verifies the "No diff" toast notification appears
   */
  verifyNoDiffToast: async (page: Page): Promise<void> => {
    return expect(page.locator('[data-testid="notification-connection.noDiff"]')).toBeVisible({ timeout: 30000 });
  },
};

/**
 * Catalog diff modal helpers
 */
export const catalogDiffModal = {
  /**
   * Verifies the catalog diff modal is visible
   */
  verifyModalVisible: async (page: Page): Promise<void> => {
    // Wait for the modal heading to be visible rather than just the container
    // The container might be in the DOM but still animating/rendering
    const modal = page.locator('[data-testid="catalog-diff-modal"]');
    return expect(modal.getByRole("heading", { name: /refreshed source schema/i })).toBeVisible({ timeout: 30000 });
  },

  /**
   * Verifies the catalog diff modal is not visible
   */
  verifyModalNotVisible: async (page: Page): Promise<void> => {
    return expect(page.locator('[data-testid="catalog-diff-modal"]')).not.toBeVisible();
  },

  /**
   * Verifies removed streams are shown in the modal
   */
  verifyRemovedStreams: async (page: Page, streamNames: string[]): Promise<void> => {
    const removedTable = page.locator('table[aria-label="removed streams table"]');
    await expect(removedTable).toBeVisible();

    for (const streamName of streamNames) {
      await expect(removedTable.locator(`text=${streamName}`)).toBeVisible();
    }
  },

  /**
   * Verifies new streams are shown in the modal
   */
  verifyNewStreams: async (page: Page, streamNames: string[]): Promise<void> => {
    const newTable = page.locator('table[aria-label="new streams table"]');
    await expect(newTable).toBeVisible();

    for (const streamName of streamNames) {
      await expect(newTable.locator(`text=${streamName}`)).toBeVisible();
    }
  },

  /**
   * Verifies that there are no removed streams in the modal
   */
  verifyNoRemovedStreams: async (page: Page): Promise<void> => {
    const removedTable = page.locator('table[aria-label="removed streams table"]');
    return expect(removedTable).not.toBeVisible();
  },

  /**
   * Verifies that there are no new streams in the modal
   */
  verifyNoNewStreams: async (page: Page): Promise<void> => {
    const newTable = page.locator('table[aria-label="new streams table"]');
    return expect(newTable).not.toBeVisible();
  },

  /**
   * Toggles the accordion for a stream with field-level changes
   */
  toggleStreamAccordion: async (page: Page, streamName: string): Promise<void> => {
    const accordionButton = page.locator(`button[data-testid="toggle-accordion-${streamName}-stream"]`);
    return accordionButton.click();
  },

  /**
   * Verifies removed fields are shown for an expanded stream
   */
  verifyRemovedFields: async (page: Page, fieldNames: string[]): Promise<void> => {
    const removedFieldsTable = page.locator('table[aria-label="removed fields"]');
    await expect(removedFieldsTable).toBeVisible();

    for (const fieldName of fieldNames) {
      await expect(removedFieldsTable.locator(`text=${fieldName}`)).toBeVisible();
    }
  },

  /**
   * Verifies new fields are shown for an expanded stream
   */
  verifyNewFields: async (page: Page, fieldNames: string[]): Promise<void> => {
    const newFieldsTable = page.locator('table[aria-label="new fields"]');
    await expect(newFieldsTable).toBeVisible();

    for (const fieldName of fieldNames) {
      await expect(newFieldsTable.locator(`text=${fieldName}`)).toBeVisible();
    }
  },

  /**
   * Verifies that there are no new fields for an expanded stream
   */
  verifyNoNewFields: async (page: Page): Promise<void> => {
    const newFieldsTable = page.locator('table[aria-label="new fields"]');
    return expect(newFieldsTable).not.toBeVisible();
  },

  /**
   * Closes the catalog diff modal
   */
  closeModal: async (page: Page): Promise<void> => {
    const closeButton = page.locator('[data-testid="update-schema-confirm-btn"]');
    await closeButton.click();
    return expect(page.locator('[data-testid="catalog-diff-modal"]')).not.toBeVisible();
  },
};

/**
 * Streams table helpers
 */
export const streamsTable = {
  /**
   * Clicks the refresh schema button
   */
  clickRefreshSchemaButton: async (page: Page): Promise<void> => {
    const refreshButton = page.locator('button[data-testid="refresh-schema-btn"]');
    return refreshButton.click();
  },

  /**
   * Filters the streams table by stream or field name
   */
  filterByStreamName: async (page: Page, streamName: string): Promise<void> => {
    const searchInput = page.locator('input[data-testid="sync-catalog-search"]');
    await searchInput.clear();
    return searchInput.fill(streamName);
  },

  /**
   * Selects a sync mode for a specific stream
   * This is complex as it requires expanding the stream row and interacting with dropdowns
   */
  selectSyncMode: async (
    page: Page,
    namespace: string,
    streamName: string,
    syncMode: SyncMode,
    destSyncMode: DestinationSyncMode
  ): Promise<void> => {
    // First, find the stream row
    // The row structure is: namespace row (depth-0), then stream rows (depth-1) as siblings
    const streamRow = page.locator(`tr[data-testid="row-depth-1-stream-${streamName}"]`);
    await expect(streamRow).toBeVisible();

    // Click to expand if not already expanded
    const expandButton = streamRow.locator('button[data-testid="expand-collapse-stream-btn"]');
    const isExpanded = await expandButton.getAttribute("aria-expanded");
    if (isExpanded !== "true") {
      await expandButton.click();
    }

    // Select sync mode from dropdown
    // Need a small delay to allow the UI to settle
    await page.waitForTimeout(1000);
    const syncModeButton = streamRow.locator('button[data-testid="sync-mode-select-listbox-button"]');
    await syncModeButton.click();

    // Wait for the dropdown options menu to appear
    const syncModeOptionsMenu = page.locator('ul[data-testid="sync-mode-select-listbox-options"]');
    await expect(syncModeOptionsMenu).toBeVisible();

    // Select the exact match (e.g., "Append" vs "Append + Deduped")
    const syncModeText = `${SYNC_MODE_STRINGS[syncMode]} | ${SYNC_MODE_STRINGS[destSyncMode]}`;
    await syncModeOptionsMenu.getByRole("option", { name: syncModeText, exact: true }).click({ force: true });
  },
};

/**
 * Replication form helpers (for saving changes with reset modal handling)
 */
export const replicationForm = {
  /**
   * Saves changes on the replication page and handles the reset modal if it appears
   * @param expectResetModal - whether to expect the reset data modal to appear
   */
  saveChanges: async (page: Page, expectResetModal: boolean = false): Promise<void> => {
    // Click the save button
    const saveButton = page.locator('button[data-testid="save-edit-button"]');
    await saveButton.click();

    if (expectResetModal) {
      // Wait for reset modal to appear
      const resetModal = page.locator('[data-testid="resetModal"]');
      await expect(resetModal).toBeVisible();

      // Select "Save without reset" option
      const saveWithoutResetRadio = page.locator('[data-testid="radio-button-tile-shouldRefresh-saveWithoutRefresh"]');
      await saveWithoutResetRadio.click();

      // Click save button in modal
      const modalSaveButton = page.locator('button[data-testid="refreshModal-save"]');
      await modalSaveButton.click();
    }

    // Wait for success notification
    await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
  },

  /**
   * Selects the non-breaking changes preference in advanced settings
   */
  selectNonBreakingChangesPreference: async (
    page: Page,
    preference: "propagate" | "ignore" | "disable"
  ): Promise<void> => {
    // Open the non-breaking changes preference dropdown (matching Cypress testid)
    const preferenceButton = page.locator('[data-testid="nonBreakingChangesPreference-listbox-button"]');
    await preferenceButton.click();

    // Select the preference option using the testid pattern from Cypress
    const optionSelector = `[data-testid="${preference}-option"]`;
    await page.locator(optionSelector).click({ force: true });
  },
};
