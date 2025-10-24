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
 * Navigates to the replication page for a specific connection
 */
export const navigateToReplicationPage = async (
  page: Page,
  workspaceId: string,
  connectionId: string
): Promise<void> => {
  await page.goto(`/workspaces/${workspaceId}/connections/${connectionId}/replication`, {
    timeout: 20000,
  });

  // Wait for the table to be visible to ensure page is fully loaded
  return expect(page.locator('table[data-testid="sync-catalog-table"]')).toBeVisible({ timeout: 10000 });
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
   * Clears the stream/field name filter input
   */
  clearFilterByStreamName: async (page: Page): Promise<void> => {
    const searchInput = page.locator('input[data-testid="sync-catalog-search"]');
    return searchInput.clear();
  },

  /**
   * Checks if the filter input is enabled
   */
  isFilterInputEnabled: async (page: Page): Promise<boolean> => {
    const searchInput = page.locator('input[data-testid="sync-catalog-search"]');
    return searchInput.isEnabled();
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

  // ============================================================================
  // Namespace operations
  // ============================================================================

  /**
   * Toggles the namespace checkbox to enable/disable all streams in the namespace
   */
  toggleNamespaceCheckbox: async (page: Page, enabled: boolean): Promise<void> => {
    const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');

    const isCurrentlyChecked = await namespaceCheckbox.isChecked();
    if (isCurrentlyChecked !== enabled) {
      // Force click needed due to potential overlays
      return namespaceCheckbox.click({ force: true, timeout: 10000 });
    }
  },

  /**
   * Checks if the namespace checkbox is enabled (clickable)
   */
  isNamespaceCheckboxEnabled: async (page: Page): Promise<boolean> => {
    const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
    return namespaceCheckbox.isEnabled();
  },

  /**
   * Checks if the namespace checkbox is checked
   */
  isNamespaceCheckboxChecked: async (page: Page): Promise<boolean> => {
    const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
    return namespaceCheckbox.isChecked();
  },

  /**
   * Checks if the namespace checkbox is in mixed/indeterminate state
   */
  isNamespaceCheckboxMixed: async (page: Page): Promise<boolean> => {
    const namespaceCheckbox = page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
    const ariaChecked = await namespaceCheckbox.getAttribute("aria-checked");
    return ariaChecked === "mixed";
  },

  /**
   * Gets the namespace stream count text (e.g., "3 streams" or "1 / 3 streams")
   */
  getNamespaceStreamCount: async (page: Page): Promise<string> => {
    const namespaceCell = page.locator("thead tr th").first();
    const text = await namespaceCell.textContent();
    return text || "";
  },

  /**
   * Checks if the namespace cell is empty
   */
  isNamespaceCellEmpty: async (page: Page): Promise<boolean> => {
    const namespaceCell = page.locator("thead tr th").first();
    const text = await namespaceCell.textContent();
    return text?.trim() === "" || false;
  },

  // ============================================================================
  // Filter tab operations
  // ============================================================================

  /**
   * Clicks a filter tab (all, enabledStreams, or disabledStreams)
   */
  clickFilterTab: async (page: Page, tab: "all" | "enabledStreams" | "disabledStreams"): Promise<void> => {
    const tabId = {
      all: "all-step",
      enabledStreams: "enabledstreams-step",
      disabledStreams: "disabledstreams-step",
    }[tab];

    const tabButton = page.locator(`button[data-id="${tabId}"]`);
    return tabButton.click({ timeout: 10000 });
  },

  /**
   * Checks if a filter tab is active (selected)
   */
  isFilterTabActive: async (page: Page, tab: "all" | "enabledStreams" | "disabledStreams"): Promise<boolean> => {
    const tabId = {
      all: "all-step",
      enabledStreams: "enabledstreams-step",
      disabledStreams: "disabledstreams-step",
    }[tab];

    const tabButton = page.locator(`button[data-id="${tabId}"]`);
    const className = await tabButton.getAttribute("class");
    return className?.includes("active") || false;
  },

  /**
   * Checks if a filter tab is enabled (clickable)
   */
  isFilterTabEnabled: async (page: Page, tab: "all" | "enabledStreams" | "disabledStreams"): Promise<boolean> => {
    const tabId = {
      all: "all-step",
      enabledStreams: "enabledstreams-step",
      disabledStreams: "disabledstreams-step",
    }[tab];

    const tabButton = page.locator(`button[data-id="${tabId}"]`);
    return tabButton.isEnabled();
  },

  // ============================================================================
  // Save/discard operations
  // ============================================================================

  /**
   * Clicks the save changes button
   */
  clickSaveChangesButton: async (page: Page): Promise<void> => {
    const saveButton = page.locator('button[data-testid="save-edit-button"]');
    return saveButton.click({ timeout: 10000 });
  },

  /**
   * Clicks the discard changes button
   */
  clickDiscardChangesButton: async (page: Page): Promise<void> => {
    const discardButton = page.locator('button[data-testid="cancel-edit-button"]');
    return discardButton.click({ timeout: 10000 });
  },

  /**
   * Checks if the save changes button is enabled
   */
  isSaveChangesButtonEnabled: async (page: Page): Promise<boolean> => {
    const saveButton = page.locator('button[data-testid="save-edit-button"]');
    return saveButton.isEnabled();
  },

  // ============================================================================
  // Error state checks
  // ============================================================================

  /**
   * Checks if the "no selected streams" error is displayed
   */
  hasNoStreamsSelectedError: async (page: Page): Promise<boolean> => {
    const errorText = page.getByText("Select at least 1 stream to sync.");
    return errorText.isVisible();
  },

  /**
   * Checks if the "no streams" empty state message is displayed
   */
  hasNoStreamsMessage: async (page: Page): Promise<boolean> => {
    const emptyStateText = page.locator('table[data-testid="sync-catalog-table"]').getByText("No streams");
    return emptyStateText.isVisible();
  },

  /**
   * Checks if the "no matching streams" message is displayed (when filter has no results)
   */
  hasNoMatchingStreamsMessage: async (page: Page): Promise<boolean> => {
    const noMatchText = page.locator('table[data-testid="sync-catalog-table"]').getByText("No matching streams");
    return noMatchText.isVisible();
  },

  // ============================================================================
  // Refresh schema operations
  // ============================================================================

  /**
   * Checks if the refresh schema button exists
   */
  refreshSchemaButtonExists: async (page: Page): Promise<boolean> => {
    const button = page.locator('button[data-testid="refresh-schema-btn"]');
    const count = await button.count();
    return count > 0;
  },

  /**
   * Checks if the refresh schema button is enabled
   */
  isRefreshSchemaButtonEnabled: async (page: Page): Promise<boolean> => {
    const button = page.locator('button[data-testid="refresh-schema-btn"]');
    return button.isEnabled();
  },

  // ============================================================================
  // Expand/collapse all operations
  // ============================================================================

  /**
   * Checks if the expand/collapse all streams button exists
   */
  expandCollapseAllStreamsButtonExists: async (page: Page): Promise<boolean> => {
    const button = page.locator('button[data-testid="expand-collapse-all-streams-btn"]');
    const count = await button.count();
    return count > 0;
  },

  /**
   * Clicks the expand/collapse all streams button
   */
  clickExpandCollapseAllStreamsButton: async (page: Page): Promise<void> => {
    const button = page.locator('button[data-testid="expand-collapse-all-streams-btn"]');
    return button.click({ timeout: 10000 });
  },

  /**
   * Checks if the expand/collapse all streams button is enabled
   */
  isExpandCollapseAllStreamsButtonEnabled: async (page: Page): Promise<boolean> => {
    const button = page.locator('button[data-testid="expand-collapse-all-streams-btn"]');
    return button.isEnabled();
  },
};

/**
 * Replication form helpers (for saving changes with reset modal handling)
 */
export const replicationForm = {
  /**
   * Saves changes on the replication page and handles the reset/refresh modal if it appears
   * @param expectResetModal - whether to expect the reset/refresh data modal to appear
   *
   * Note: The modal can be either a "refresh" modal or "reset" modal depending on the configuration.
   * This handles both cases.
   */
  saveChanges: async (page: Page, expectResetModal: boolean = false): Promise<void> => {
    // Click the save button
    const saveButton = page.locator('button[data-testid="save-edit-button"]');
    await saveButton.click();

    if (expectResetModal) {
      // Handle either refresh modal or reset modal (CI vs local differences)
      const refreshModalSave = page.locator('[data-testid="refreshModal-save"]');
      const resetModalSave = page.locator('[data-testid="resetModal-save"]');

      // Wait for either modal to appear
      let isRefreshModal = false;
      try {
        // Try refresh modal first
        await refreshModalSave.waitFor({ state: "visible", timeout: 15000 });
        isRefreshModal = true;
      } catch {
        // If refresh modal not found, try reset modal
        await resetModalSave.waitFor({ state: "visible", timeout: 15000 });
      }

      if (isRefreshModal) {
        // Refresh modal: Select "Save without refresh" option
        const saveWithoutRefreshRadio = page.locator(
          '[data-testid="radio-button-tile-shouldRefresh-saveWithoutRefresh"]'
        );
        await saveWithoutRefreshRadio.click();
        await refreshModalSave.click();
      } else {
        // Reset modal: Select "Save without reset" option
        const saveWithoutResetRadio = page.locator('[data-testid="radio-button-tile-shouldClear-saveWithoutClear"]');
        await saveWithoutResetRadio.click({ force: true });
        await resetModalSave.click();
      }
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
