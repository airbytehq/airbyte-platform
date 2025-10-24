/**
 * Stream Row helpers for Playwright tests
 * Provides functions for interacting with individual stream rows in the sync catalog table
 */

import { Page, Locator, expect } from "@playwright/test";
import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

/**
 * Sync mode display strings that match the UI format
 */
const SYNC_MODE_STRINGS: Record<SyncMode | DestinationSyncMode, string> = {
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
 * Helper functions for interacting with stream rows
 */
export const streamRow = {
  // ============================================================================
  // Base locator helpers
  // ============================================================================

  /**
   * Gets the stream row locator for a specific namespace and stream
   * This is the base locator used by all other functions
   */
  getStreamRow: (page: Page, namespace: string, streamName: string): Locator => {
    return page.locator(`tr[data-testid="row-depth-1-stream-${streamName}"]`);
  },

  /**
   * Gets the field row locator for a specific field within a stream
   */
  getFieldRow: (page: Page, namespace: string, streamName: string, fieldName: string): Locator => {
    // Field rows are siblings of the stream row
    return page.locator(`tr[data-testid="row-depth-2-field-${fieldName}"]`);
  },

  // ============================================================================
  // Cell/element locator helpers
  // ============================================================================

  /**
   * Gets the primary key cell locator for a stream
   */
  getPKCell: (page: Page, namespace: string, streamName: string): Locator => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    return streamRowLoc.locator('div[data-testid="primary-key-cell"]');
  },

  /**
   * Gets the cursor field cell locator for a stream
   */
  getCursorCell: (page: Page, namespace: string, streamName: string): Locator => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    return streamRowLoc.locator('div[data-testid="cursor-field-cell"]');
  },

  /**
   * Gets the sync mode button locator for a stream
   */
  getSyncModeButton: (page: Page, namespace: string, streamName: string): Locator => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    return streamRowLoc.locator('button[data-testid="sync-mode-select-listbox-button"]');
  },

  /**
   * Gets the stream sync checkbox locator
   */
  getStreamCheckbox: (page: Page, namespace: string, streamName: string): Locator => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    return streamRowLoc.locator('input[data-testid="sync-stream-checkbox"]');
  },

  /**
   * Gets the field sync checkbox locator
   */
  getFieldCheckbox: (page: Page, namespace: string, streamName: string, fieldName: string): Locator => {
    const fieldRowLoc = streamRow.getFieldRow(page, namespace, streamName, fieldName);
    return fieldRowLoc.locator('input[data-testid="sync-field-checkbox"]');
  },

  // ============================================================================
  // Stream-level operations
  // ============================================================================

  /**
   * Toggles the stream sync checkbox to enable or disable the stream
   */
  toggleStreamSync: async (page: Page, namespace: string, streamName: string, enabled: boolean): Promise<void> => {
    const checkbox = streamRow.getStreamCheckbox(page, namespace, streamName);
    const isCurrentlyChecked = await checkbox.isChecked();

    if (isCurrentlyChecked !== enabled) {
      // Force click needed due to potential overlays
      return checkbox.click({ force: true, timeout: 10000 });
    }
  },

  /**
   * Checks if the stream sync checkbox is enabled (checked)
   */
  isStreamSyncEnabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const checkbox = streamRow.getStreamCheckbox(page, namespace, streamName);
    return checkbox.isChecked();
  },

  /**
   * Checks if the stream sync checkbox is disabled (not clickable)
   */
  isStreamSyncCheckboxDisabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const checkbox = streamRow.getStreamCheckbox(page, namespace, streamName);
    return checkbox.isDisabled();
  },

  // ============================================================================
  // Expand/collapse operations
  // ============================================================================

  /**
   * Toggles the expand/collapse state of a stream row to show/hide fields
   */
  toggleExpandCollapse: async (page: Page, namespace: string, streamName: string): Promise<void> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    const expandButton = streamRowLoc.locator('button[data-testid="expand-collapse-stream-btn"]');
    return expandButton.click({ timeout: 10000 });
  },

  /**
   * Checks if the stream is currently expanded
   */
  isExpanded: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    const expandButton = streamRowLoc.locator('button[data-testid="expand-collapse-stream-btn"]');
    const ariaExpanded = await expandButton.getAttribute("aria-expanded");
    return ariaExpanded === "true";
  },

  /**
   * Checks if the expand/collapse button is enabled (clickable)
   */
  isExpandButtonEnabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    const expandButton = streamRowLoc.locator('button[data-testid="expand-collapse-stream-btn"]');
    return expandButton.isEnabled();
  },

  // ============================================================================
  // Sync mode operations
  // ============================================================================

  /**
   * Selects a sync mode for the stream from the dropdown
   */
  selectSyncMode: async (
    page: Page,
    namespace: string,
    streamName: string,
    source: SyncMode,
    dest: DestinationSyncMode
  ): Promise<void> => {
    const syncModeButton = streamRow.getSyncModeButton(page, namespace, streamName);

    await syncModeButton.click({ timeout: 10000 });

    const menu = page.locator('ul[data-testid="sync-mode-select-listbox-options"]');
    await expect(menu).toBeVisible({ timeout: 10000 });

    // Build the display string that matches UI format
    const syncModeText = `${SYNC_MODE_STRINGS[source]} | ${SYNC_MODE_STRINGS[dest]}`;
    const option = menu.getByRole("option", { name: syncModeText, exact: true });

    return option.click({ timeout: 5000 });
  },

  /**
   * Checks if the sync mode dropdown is disabled
   */
  isSyncModeDropdownDisabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const syncModeButton = streamRow.getSyncModeButton(page, namespace, streamName);
    return syncModeButton.isDisabled();
  },

  /**
   * Checks if the sync mode dropdown is displayed
   */
  isSyncModeDropdownDisplayed: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const syncModeButton = streamRow.getSyncModeButton(page, namespace, streamName);
    return syncModeButton.isVisible();
  },

  /**
   * Gets the currently selected sync mode display text
   */
  getSelectedSyncModeText: async (page: Page, namespace: string, streamName: string): Promise<string> => {
    const syncModeButton = streamRow.getSyncModeButton(page, namespace, streamName);
    const text = await syncModeButton.textContent();
    return text || "";
  },

  /**
   * Verifies that the selected sync mode matches the expected values
   */
  verifySelectedSyncMode: async (
    page: Page,
    namespace: string,
    streamName: string,
    source: SyncMode,
    dest: DestinationSyncMode
  ): Promise<void> => {
    const syncModeButton = streamRow.getSyncModeButton(page, namespace, streamName);

    await expect(syncModeButton).toContainText(SYNC_MODE_STRINGS[source]);
    return expect(syncModeButton).toContainText(SYNC_MODE_STRINGS[dest]);
  },

  // ============================================================================
  // Primary Key operations
  // ============================================================================

  /**
   * Selects primary keys from the multi-select dropdown
   * Note: PK dropdown is multi-select and must be closed manually after selections
   */
  selectPKs: async (page: Page, namespace: string, streamName: string, pks: string[]): Promise<void> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    const button = pkCell.locator("button");

    await button.click({ timeout: 10000 });

    const listbox = page.locator('div[role="listbox"]');
    await expect(listbox).toBeVisible({ timeout: 10000 });

    // Select each PK option
    for (const pk of pks) {
      const option = listbox.getByRole("option", { name: pk, exact: true });
      await option.click({ timeout: 5000 });
    }

    // Close the multi-select dropdown with ESC key
    return pkCell.press("Escape");
  },

  /**
   * Gets the selected primary key display text
   */
  getSelectedPK: async (page: Page, namespace: string, streamName: string): Promise<string> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    const text = await pkCell.textContent();
    return text || "";
  },

  /**
   * Verifies the selected PK matches expected text
   */
  verifySelectedPK: async (page: Page, namespace: string, streamName: string, expectedText: string): Promise<void> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    return expect(pkCell).toHaveText(expectedText);
  },

  /**
   * Checks if the PK combobox button is disabled
   */
  isPKComboboxDisabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    const button = pkCell.locator("button");
    return button.isDisabled();
  },

  /**
   * Checks if the PK combobox button is displayed
   */
  isPKComboboxDisplayed: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    const button = pkCell.locator("button");
    return button.isVisible();
  },

  /**
   * Checks if the "Primary key missing" error is displayed
   */
  hasMissingPKError: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const pkCell = streamRow.getPKCell(page, namespace, streamName);
    const errorText = pkCell.getByText("Primary key missing");
    return errorText.isVisible();
  },

  // ============================================================================
  // Cursor operations
  // ============================================================================

  /**
   * Selects a cursor field from the dropdown
   */
  selectCursor: async (page: Page, namespace: string, streamName: string, cursor: string): Promise<void> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    const button = cursorCell.locator("button");

    await button.click({ timeout: 10000 });

    const listbox = page.locator('div[role="listbox"]');
    await expect(listbox).toBeVisible({ timeout: 10000 });

    const option = listbox.getByRole("option", { name: cursor, exact: true });
    return option.click({ timeout: 5000 });
  },

  /**
   * Gets the selected cursor field display text
   */
  getSelectedCursor: async (page: Page, namespace: string, streamName: string): Promise<string> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    const text = await cursorCell.textContent();
    return text || "";
  },

  /**
   * Verifies the selected cursor matches expected text
   */
  verifySelectedCursor: async (
    page: Page,
    namespace: string,
    streamName: string,
    expectedText: string
  ): Promise<void> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    return expect(cursorCell).toHaveText(expectedText);
  },

  /**
   * Checks if the cursor combobox button is disabled
   */
  isCursorComboboxDisabled: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    const button = cursorCell.locator("button");
    return button.isDisabled();
  },

  /**
   * Checks if the cursor combobox button is displayed
   */
  isCursorComboboxDisplayed: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    const button = cursorCell.locator("button");
    return button.isVisible();
  },

  /**
   * Checks if the "Cursor missing" error is displayed
   */
  hasMissingCursorError: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const cursorCell = streamRow.getCursorCell(page, namespace, streamName);
    const errorText = cursorCell.getByText("Cursor missing");
    return errorText.isVisible();
  },

  // ============================================================================
  // Field-level operations
  // ============================================================================

  /**
   * Toggles a field's sync checkbox to enable or disable it
   */
  toggleFieldSync: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string,
    enabled: boolean
  ): Promise<void> => {
    const checkbox = streamRow.getFieldCheckbox(page, namespace, streamName, fieldName);
    const isCurrentlyChecked = await checkbox.isChecked();

    if (isCurrentlyChecked !== enabled) {
      // Force click needed due to potential overlays
      return checkbox.click({ force: true, timeout: 10000 });
    }
  },

  /**
   * Checks if a field's sync checkbox is enabled (checked)
   */
  isFieldSyncEnabled: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string
  ): Promise<boolean> => {
    const checkbox = streamRow.getFieldCheckbox(page, namespace, streamName, fieldName);
    return checkbox.isChecked();
  },

  /**
   * Checks if a field's sync checkbox is disabled (not clickable)
   */
  isFieldSyncCheckboxDisabled: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string
  ): Promise<boolean> => {
    const checkbox = streamRow.getFieldCheckbox(page, namespace, streamName, fieldName);
    return checkbox.isDisabled();
  },

  /**
   * Checks if a field's sync checkbox is displayed
   */
  isFieldSyncCheckboxDisplayed: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string
  ): Promise<boolean> => {
    const checkbox = streamRow.getFieldCheckbox(page, namespace, streamName, fieldName);
    return checkbox.isVisible();
  },

  /**
   * Checks if a field is marked as a primary key
   */
  isFieldPK: async (page: Page, namespace: string, streamName: string, fieldName: string): Promise<boolean> => {
    const fieldRowLoc = streamRow.getFieldRow(page, namespace, streamName, fieldName);
    const pkCell = fieldRowLoc.locator('div[data-testid="field-pk-cell"]');
    const pkText = await pkCell.textContent();
    return pkText?.includes("primary key") || false;
  },

  /**
   * Checks if a field is marked as a cursor
   */
  isFieldCursor: async (page: Page, namespace: string, streamName: string, fieldName: string): Promise<boolean> => {
    const fieldRowLoc = streamRow.getFieldRow(page, namespace, streamName, fieldName);
    const cursorCell = fieldRowLoc.locator('div[data-testid="field-cursor-cell"]');
    const cursorText = await cursorCell.textContent();
    return cursorText?.includes("cursor") || false;
  },

  /**
   * Checks if a field's type is displayed correctly
   */
  verifyFieldType: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string,
    expectedType: string
  ): Promise<void> => {
    const fieldRowLoc = streamRow.getFieldRow(page, namespace, streamName, fieldName);
    return expect(fieldRowLoc).toContainText(expectedType);
  },

  // ============================================================================
  // Style verification operations
  // ============================================================================

  /**
   * Checks if a stream row has a specific style class (added/removed/changed/disabled)
   */
  hasStreamStyle: async (
    page: Page,
    namespace: string,
    streamName: string,
    style: "added" | "removed" | "changed" | "disabled"
  ): Promise<boolean> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    const className = await streamRowLoc.getAttribute("class");
    return className?.includes(style) || false;
  },

  /**
   * Checks if a field row has a specific style class (added/removed/disabled)
   */
  hasFieldStyle: async (
    page: Page,
    namespace: string,
    streamName: string,
    fieldName: string,
    style: "added" | "removed" | "disabled"
  ): Promise<boolean> => {
    const fieldRowLoc = streamRow.getFieldRow(page, namespace, streamName, fieldName);
    const className = await fieldRowLoc.getAttribute("class");
    return className?.includes(style) || false;
  },

  // ============================================================================
  // Visibility operations
  // ============================================================================

  /**
   * Checks if a stream row is visible in the table
   */
  isStreamVisible: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    return streamRowLoc.isVisible();
  },

  /**
   * Checks if a stream row exists in the DOM (may not be visible)
   */
  streamExists: async (page: Page, namespace: string, streamName: string): Promise<boolean> => {
    const streamRowLoc = streamRow.getStreamRow(page, namespace, streamName);
    const count = await streamRowLoc.count();
    return count > 0;
  },
};
