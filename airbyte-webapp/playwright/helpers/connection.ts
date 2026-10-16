import { Page, expect } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

/**
 * Connection Test Helpers
 *
 * This file contains a set of helpers for wiring up connections in our Playwright tests:
 *
 * - connectionUI: UI navigation and interaction helpers (visit pages, start/cancel syncs, verify status)
 * - jobUI: Job event and modal helpers for timeline/status tests
 * - connectionForm: Form interaction helpers for connection settings page
 */

// UI navigation helpers for connection pages
export const connectionUI = {
  // Visit a specific connection page tab
  visit: async (page: Page, connection: WebBackendConnectionRead, tab: string = ""): Promise<void> => {
    // Get workspaceId from the source object
    const workspaceId = connection.source.workspaceId;
    const connectionId = connection.connectionId;

    let url = `/workspaces/${workspaceId}/connections/${connectionId}`;
    if (tab) {
      url += `/${tab}`;
    }

    await page.goto(url, { timeout: 10000 });
  },

  // Start a manual sync
  startManualSync: async (page: Page): Promise<void> => {
    return page.locator("[data-testid='manual-sync-button']").click();
  },

  // Cancel an ongoing sync
  cancelSync: async (page: Page): Promise<void> => {
    await page.locator("[data-testid='cancel-sync-button']").click();
    return page.locator("text=Yes, cancel sync").click();
  },

  // Verify sync status messages
  verifySyncStatus: {
    running: async (page: Page): Promise<void> => {
      await expect(page.locator("text=manually started a sync")).toBeVisible({ timeout: 10000 });
      return expect(page.locator("text=Sync running")).toBeVisible({ timeout: 10000 });
    },
    cancelled: async (page: Page): Promise<void> => {
      return expect(page.locator("text=Sync cancelled")).toBeVisible({ timeout: 10000 });
    },
  },
};

// Job event and modal helpers for timeline/status tests
export const jobUI = {
  // Open job event menu
  openEventMenu: async (page: Page): Promise<void> => {
    const eventMenuButton = page.locator("[data-testid='job-event-menu']").locator("button");
    await eventMenuButton.waitFor({ state: "visible", timeout: 10000 });
    return eventMenuButton.click();
  },

  // Copy link to event from menu
  copyEventLink: async (page: Page): Promise<string> => {
    await page.locator("[data-testid='copy-link-to-event']").click({ force: true });
    return await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
  },

  // Open job logs modal from menu
  openLogsFromMenu: async (page: Page): Promise<void> => {
    await page.locator("[data-testid='view-logs']").click();
    return expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 15000 });
  },

  // Copy attempt link from modal
  copyAttemptLink: async (page: Page): Promise<string> => {
    await page.locator("[data-testid='copy-link-to-attempt-button']").click();
    return await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
  },

  // Close job logs modal
  closeLogsModal: async (page: Page): Promise<void> => {
    return page.locator("[data-testid='close-modal-button']").click();
  },

  // Verify job logs modal is open
  verifyLogsModal: async (page: Page): Promise<void> => {
    return expect(page.locator("[data-testid='job-logs-modal']")).toBeVisible({ timeout: 30000 });
  },
};

// Connection form helpers for settings page interactions
export const connectionForm = {
  // Toggle the advanced settings section
  toggleAdvancedSettings: async (page: Page): Promise<void> => {
    const button = page.locator('[data-testid="advanced-settings-button"]');
    await button.click();
    // Wait for advanced settings content to expand
    // Use a simple timeout since fields may vary based on connection state
    await page.waitForTimeout(1000);
  },

  // Select schedule type: "Scheduled", "Manual", or "Cron"
  selectScheduleType: async (page: Page, scheduleType: "Scheduled" | "Manual" | "Cron"): Promise<void> => {
    const button = page.locator('[data-testid="schedule-type-listbox-button"]');

    // Click to open the dropdown
    await button.click();

    // Wait for dropdown to be visible
    await page.waitForTimeout(300);

    // Click directly on the desired option
    const option = page.locator(`[data-testid="${scheduleType.toLowerCase()}-option"]`);
    await option.waitFor({ state: "visible", timeout: 5000 });
    await option.click();

    // Wait for dropdown to close and verify selection
    await expect(button).toContainText(scheduleType, { timeout: 5000 });
  },

  // Setup destination namespace with custom format
  setupDestinationNamespaceCustomFormat: async (page: Page, customValue: string): Promise<void> => {
    const namespaceButton = page.locator('[data-testid="namespace-definition-listbox-button"]');
    await namespaceButton.waitFor({ state: "visible", timeout: 10000 });
    await namespaceButton.click();
    await page.locator('[data-testid="custom-option"]').click();
    const input = page.locator('[data-testid="namespace-definition-custom-format-input"]').first();
    // Fill with the full format including ${SOURCE_NAMESPACE} prefix
    await input.fill(`\${SOURCE_NAMESPACE}${customValue}`);
  },

  // Setup destination namespace with source-defined option
  setupDestinationNamespaceSourceFormat: async (page: Page): Promise<void> => {
    await page.locator('[data-testid="namespace-definition-listbox-button"]').click();
    return page.locator('[data-testid="source-option"]').click({ force: true });
  },

  // Setup destination namespace with destination default option
  setupDestinationNamespaceDestinationFormat: async (page: Page): Promise<void> => {
    const namespaceButton = page.locator('[data-testid="namespace-definition-listbox-button"]');
    await namespaceButton.waitFor({ state: "visible", timeout: 10000 });
    await namespaceButton.click();
    return page.locator('[data-testid="destination-option"]').click({ force: true });
  },

  // Set stream prefix
  setStreamPrefix: async (page: Page, prefix: string): Promise<void> => {
    const input = page.locator('[data-testid="stream-prefix-input"]');
    return input.fill(prefix);
  },

  // Clear stream prefix
  clearStreamPrefix: async (page: Page): Promise<void> => {
    const input = page.locator('[data-testid="stream-prefix-input"]');
    return input.clear();
  },

  // Submit the form
  submit: async (page: Page): Promise<void> => {
    return page.locator("button[type='submit']").click();
  },

  // Wait for and verify success notification
  verifySuccessNotification: async (page: Page): Promise<void> => {
    return expect(page.locator("text=Your changes were saved!")).toBeVisible({ timeout: 10000 });
  },

  // Verify a preview element contains expected text
  verifyPreview: async (page: Page, testId: string, expectedText: string): Promise<void> => {
    const preview = page.locator(`[data-testid="${testId}"]`);
    return expect(preview).toContainText(expectedText, { timeout: 5000 });
  },

  // Verify a preview element is not visible
  verifyPreviewNotVisible: async (page: Page, testId: string): Promise<void> => {
    const preview = page.locator(`[data-testid="${testId}"]`);
    return expect(preview).not.toBeVisible({ timeout: 10000 });
  },
};
