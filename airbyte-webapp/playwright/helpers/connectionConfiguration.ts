import { Page, expect } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

import { connectionUI } from "./connection";

/**
 * Connection Configuration Test Helpers
 *
 * This file contains helpers specific to connection configuration/settings page tests.
 * For generic connection helpers, see connection.ts
 */

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
};
