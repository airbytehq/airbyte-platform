/**
 * Connection list page helpers for Playwright tests
 * Handles connection list table, status indicators, and schema change icons
 */

import { Page, Locator, expect } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

/**
 * Connection list page helpers
 */
export const connectionList = {
  /**
   * Navigates to the connection list page for a workspace
   */
  visit: async (page: Page, workspaceId: string): Promise<void> => {
    // Wait for connections list API call to complete
    const listPromise = page.waitForResponse((response) => response.url().includes("/web_backend/connections/list"), {
      timeout: 20000,
    });

    await page.goto(`/workspaces/${workspaceId}/connections`);
    await listPromise;

    // Wait for URL to confirm navigation
    return expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/connections`));
  },

  /**
   * Gets the schema change icon for a specific connection
   * @param type - "warning" for non-breaking changes, "error" for breaking changes
   * @returns Locator for the icon element
   */
  getSchemaChangeIcon: (page: Page, connection: WebBackendConnectionRead, type: "warning" | "error"): Locator => {
    // Schema change icons appear within the replication link cell
    // They use data-testid like "entitywarnings-warning" or "entitywarnings-error"
    const replicationCell = page.locator(`[data-testid='link-replication-${connection.connectionId}']`);
    return replicationCell.locator(`[data-testid='entitywarnings-${type}']`);
  },

  /**
   * Gets the connection status switch for a specific connection
   * @returns Locator for the switch element
   */
  getConnectionSwitch: (page: Page, connection: WebBackendConnectionRead): Locator => {
    // Note: On the list page, the switch has a different data-testid pattern than on the connection page
    return page.locator(`[data-testid='connection-state-switch-${connection.connectionId}']`);
  },

  /**
   * Verifies a connection has a schema change warning icon (non-breaking changes)
   */
  verifyHasWarningIcon: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const warningIcon = connectionList.getSchemaChangeIcon(page, connection, "warning");
    return expect(warningIcon).toBeVisible();
  },

  /**
   * Verifies a connection does NOT have a schema change warning icon
   */
  verifyNoWarningIcon: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const warningIcon = connectionList.getSchemaChangeIcon(page, connection, "warning");
    return expect(warningIcon).not.toBeVisible();
  },

  /**
   * Verifies a connection has a schema change error icon (breaking changes)
   */
  verifyHasErrorIcon: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const errorIcon = connectionList.getSchemaChangeIcon(page, connection, "error");
    return expect(errorIcon).toBeVisible();
  },

  /**
   * Verifies a connection does NOT have a schema change error icon
   */
  verifyNoErrorIcon: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const errorIcon = connectionList.getSchemaChangeIcon(page, connection, "error");
    return expect(errorIcon).not.toBeVisible();
  },

  /**
   * Verifies the connection switch is enabled and checked
   */
  verifyConnectionEnabled: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const connectionSwitch = connectionList.getConnectionSwitch(page, connection);
    await expect(connectionSwitch).toBeChecked();
    return expect(connectionSwitch).toBeEnabled();
  },

  /**
   * Verifies the connection switch is disabled and unchecked (for breaking changes)
   */
  verifyConnectionDisabled: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const connectionSwitch = connectionList.getConnectionSwitch(page, connection);
    await expect(connectionSwitch).not.toBeChecked();
    return expect(connectionSwitch).toBeDisabled();
  },
};
