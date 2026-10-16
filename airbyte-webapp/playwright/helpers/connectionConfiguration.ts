import { Page, expect } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

import { connectionUI } from "./connection";

/**
 * Connection Configuration Test Helpers
 *
 * This file contains helpers specific to connection configuration/settings page tests.
 * For generic connection helpers, see connection.ts
 */

type ConnectionTab = "" | "status" | "settings" | "replication" | "timeline";

// High-level connection settings test helpers
export const connectionSettings = {
  // Verify element is disabled on deleted connection pages
  verifyElementDisabled: async (page: Page, testId: string): Promise<void> => {
    const element = page.locator(`[data-testid="${testId}"]`);
    return expect(element).toBeDisabled({ timeout: 10000 });
  },

  // Verify element is not visible
  verifyElementNotVisible: async (page: Page, selector: string): Promise<void> => {
    const element = page.locator(selector);
    return expect(element).not.toBeVisible({ timeout: 10000 });
  },
};

// Connection deletion helpers
export const connectionDeletion = {
  // Complete connection deletion flow
  deleteConnection: async (page: Page, workspaceId: string, connection: WebBackendConnectionRead): Promise<void> => {
    await connectionUI.visit(page, connection, "settings");
    await expect(page.locator("button[type='submit']")).toBeVisible({ timeout: 10000 });

    const deleteButton = page.locator('button[data-id="open-delete-modal"]');
    await expect(deleteButton).toBeVisible({ timeout: 10000 });
    await deleteButton.click();

    const confirmationInput = page.locator('input[id="confirmation-text"]');
    await expect(confirmationInput).toBeVisible({ timeout: 10000 });

    const placeholder = await confirmationInput.getAttribute("placeholder");
    if (placeholder) {
      await confirmationInput.fill(placeholder);
    }

    const confirmDeleteButton = page.locator('button[data-id="delete"]');
    await expect(confirmDeleteButton).toBeEnabled({ timeout: 5000 });
    await confirmDeleteButton.click();

    return expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/connections`), { timeout: 15000 });
  },

  // Navigate to a deleted connection page
  navigateToDeleted: async (
    page: Page,
    workspaceId: string,
    connectionId: string,
    tab: ConnectionTab
  ): Promise<void> => {
    const url = tab
      ? `/workspaces/${workspaceId}/connections/${connectionId}/${tab}`
      : `/workspaces/${workspaceId}/connections/${connectionId}`;
    await page.goto(url, { timeout: 20000 });
  },

  // Verify deleted connection warning message
  verifyDeletedMessage: async (page: Page): Promise<void> => {
    const deletedMessage = page
      .locator("div")
      .filter({ hasText: /^This connection has been deleted/ })
      .first();
    return expect(deletedMessage).toBeVisible({ timeout: 10000 });
  },
};
