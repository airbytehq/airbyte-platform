import { Page, expect, Request } from "@playwright/test";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

import { connectionUI, connectionForm, connectionAPI } from "./connection";

/**
 * Connection Configuration Test Helpers
 *
 * This file contains helpers specific to connection configuration/settings page tests.
 * For generic connection helpers, see connection.ts
 */

type ConnectionTab = "" | "status" | "settings" | "replication" | "timeline";

// API interceptor helpers
export const apiInterceptors = {
  // Setup interceptor for get connection requests
  setupGetConnectionIntercept: async (page: Page): Promise<Request[]> => {
    const requests: Request[] = [];
    await page.route("**/api/v1/web_backend/connections/get", async (route) => {
      requests.push(route.request());
      await route.continue();
    });
    return requests;
  },

  // Setup interceptor for update connection requests
  setupUpdateConnectionIntercept: async (page: Page): Promise<Request[]> => {
    const requests: Request[] = [];
    await page.route("**/api/v1/web_backend/connections/update", async (route) => {
      requests.push(route.request());
      await route.continue();
    });
    return requests;
  },
};

// High-level connection settings test helpers
export const connectionSettings = {
  // Navigate to settings and wait for form to be ready
  navigateAndWaitForForm: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    await connectionUI.visit(page, connection, "settings");
    const submitButton = page.locator("button[type='submit']");
    return expect(submitButton).toBeVisible({ timeout: 10000 });
  },

  // Navigate to settings, wait for form, and wait for connection data to load
  navigateAndWaitForConnection: async (
    page: Page,
    connection: WebBackendConnectionRead,
    getConnectionRequests: Request[]
  ): Promise<void> => {
    await connectionUI.visit(page, connection, "settings");
    const scheduleTypeButton = page.locator('[data-testid="schedule-type-listbox-button"]');
    await expect(scheduleTypeButton).toBeVisible({ timeout: 10000 });
    return expect.poll(() => getConnectionRequests.length, { timeout: 10000 }).toBeGreaterThanOrEqual(1);
  },

  // Submit form and wait for update to complete
  submitAndWaitForUpdate: async (page: Page, updateRequests: Request[]): Promise<void> => {
    const submitButton = page.locator("button[type='submit']");
    await expect(submitButton).toBeEnabled({ timeout: 5000 });
    await connectionForm.submit(page);
    await expect.poll(() => updateRequests.length, { timeout: 10000 }).toBeGreaterThanOrEqual(1);
    return connectionForm.verifySuccessNotification(page);
  },

  // Verify element is disabled on deleted connection pages
  verifyElementDisabled: async (page: Page, testId: string): Promise<void> => {
    const element = page.locator(`[data-testid="${testId}"]`);
    return expect(element).toBeDisabled({ timeout: 10000 });
  },

  // Verify element is not visible
  verifyElementNotVisible: async (page: Page, selector: string): Promise<void> => {
    const element = page.locator(selector);
    return expect(element).not.toBeVisible();
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

// High-level form update workflows that combine multiple steps
export const connectionWorkflows = {
  /**
   * Complete workflow: navigate to settings, update a field, submit, wait for success
   * Returns the request body so the caller can perform additional assertions
   */
  updateConnection: async <T = Record<string, unknown>>(
    page: Page,
    connection: WebBackendConnectionRead,
    updateAction: (page: Page) => Promise<void>,
    options: {
      toggleAdvanced?: boolean;
    } = {}
  ): Promise<T> => {
    const updateRequests = await apiInterceptors.setupUpdateConnectionIntercept(page);

    await connectionSettings.navigateAndWaitForForm(page, connection);

    if (options.toggleAdvanced) {
      await connectionForm.toggleAdvancedSettings(page);
    }

    await updateAction(page);
    await connectionSettings.submitAndWaitForUpdate(page, updateRequests);

    return await updateRequests[0].postDataJSON();
  },

  /**
   * Update destination namespace with custom format
   * Complete flow with preview verification and request body assertion
   */
  updateNamespaceCustom: async (
    page: Page,
    connection: WebBackendConnectionRead,
    customValue: string,
    expectedPreview: string,
    expectedFormat: string
  ): Promise<void> => {
    const requestBody = await connectionWorkflows.updateConnection(
      page,
      connection,
      async (page) => {
        await connectionForm.setupDestinationNamespaceCustomFormat(page, customValue);
        await connectionForm.verifyPreview(page, "custom-namespace-preview", expectedPreview);
      },
      { toggleAdvanced: true }
    );

    expect(requestBody.namespaceDefinition).toBe("customformat");
    expect(requestBody.namespaceFormat).toBe(expectedFormat);
  },

  /**
   * Update destination namespace to destination default
   */
  updateNamespaceDestinationDefault: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    const requestBody = await connectionWorkflows.updateConnection(
      page,
      connection,
      async (page) => {
        await connectionForm.setupDestinationNamespaceDestinationFormat(page);
      },
      { toggleAdvanced: true }
    );

    expect(requestBody.namespaceDefinition).toBe("destination");
  },

  /**
   * Update stream prefix with preview verification
   */
  updateStreamPrefix: async (
    page: Page,
    connection: WebBackendConnectionRead,
    prefix: string,
    customNamespace?: string
  ): Promise<void> => {
    const requestBody = await connectionWorkflows.updateConnection(
      page,
      connection,
      async (page) => {
        await connectionForm.setStreamPrefix(page, prefix);
        await connectionForm.verifyPreview(page, "stream-prefix-preview", prefix);

        if (customNamespace) {
          await connectionForm.setupDestinationNamespaceCustomFormat(page, customNamespace);
        }
      },
      { toggleAdvanced: true }
    );

    expect(requestBody.prefix).toBe(prefix);
  },

  /**
   * Clear stream prefix
   */
  clearStreamPrefix: async (page: Page, connection: WebBackendConnectionRead): Promise<void> => {
    // First set prefix via API
    const request = page.request;
    await connectionAPI.update(request, connection.connectionId, {
      prefix: "test_prefix",
    });

    const requestBody = await connectionWorkflows.updateConnection(
      page,
      connection,
      async (page) => {
        await connectionForm.clearStreamPrefix(page);
        await connectionForm.verifyPreviewNotVisible(page, "stream-prefix-preview");
      },
      { toggleAdvanced: true }
    );

    expect(requestBody.prefix).toBe("");
  },
};
