import { Page } from "@playwright/test";

// This file contains mocking helpers to handle endpoint calls we don't want
// to make in our tests. This allows us to avoid adding complex handling of random server failures on
// non-essential calls, or calls to external APIs that we don't control.

export const mockHelpers = {
  // Mock enterprise connector stubs (used by both source and destination)
  mockEnterpriseConnectors: async (page: Page) => {
    await page.route("**/api/v1/source_definitions/list_enterprise_stubs_for_workspace", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ enterpriseConnectorStubs: [] }),
      });
    });

    await page.route("**/api/v1/destination_definitions/list_enterprise_stubs_for_workspace", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ enterpriseConnectorStubs: [] }),
      });
    });
  },

  // Mock successful connection check
  mockConnectionCheck: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint =
      connectorType === "source"
        ? "**/api/v1/scheduler/sources/check_connection"
        : "**/api/v1/scheduler/destinations/check_connection";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          status: "succeeded",
          message: "Connection test succeeded",
          jobInfo: {
            id: "test-job-id",
            configType: `check_connection_${connectorType}`,
            createdAt: Date.now(),
            endedAt: Date.now() + 1000,
            succeeded: true,
            connectorConfigurationUpdated: false,
            logs: { logLines: [] },
          },
        }),
      });
    });
  },

  // Mock failed connection check
  mockFailedConnectionCheck: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint =
      connectorType === "source"
        ? "**/api/v1/scheduler/sources/check_connection"
        : "**/api/v1/scheduler/destinations/check_connection";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          status: "failed",
          message: "Something went wrong",
          jobInfo: {
            id: "test-job-id",
            configType: `check_connection_${connectorType}`,
            createdAt: Date.now(),
            endedAt: Date.now() + 1000,
            succeeded: false,
            connectorConfigurationUpdated: false,
            logs: { logLines: [] },
          },
        }),
      });
    });
  },

  // Mock empty connector lists for redirect tests
  mockEmptyConnectorLists: async (page: Page, connectorType: "source" | "destination") => {
    const endpoint = connectorType === "source" ? "**/api/v1/sources/list" : "**/api/v1/destinations/list";
    const responseKey = connectorType === "source" ? "sources" : "destinations";

    await page.route(endpoint, (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ [responseKey]: [] }),
      });
    });
  },
};
