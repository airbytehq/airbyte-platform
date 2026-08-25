import { APIRequestContext, Page, request } from "@playwright/test";

import { getApiBaseUrl } from "./api";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000000";

const fetchWorkspaceId = async (requestContext: APIRequestContext, apiBaseUrl: string): Promise<string> => {
  const response = await requestContext.post(`${apiBaseUrl}/workspaces/list_by_organization_id`, {
    data: {
      organizationId: DEFAULT_ORGANIZATION_ID,
      pagination: { pageSize: 1, rowOffset: 0 },
    },
  });

  if (!response.ok()) {
    throw new Error("Failed to get workspace ID");
  }

  const { workspaces } = await response.json();
  if (!workspaces || workspaces.length === 0) {
    throw new Error("No workspaces found");
  }

  return workspaces[0].workspaceId;
};

export const getWorkspaceId = async (page: Page): Promise<string> => {
  const serverHost = process.env.AIRBYTE_SERVER_HOST;
  const apiBaseUrl = serverHost ? `${serverHost}/api/v1` : `${page.url().split("/").slice(0, 3).join("/")}/api/v1`;

  return fetchWorkspaceId(page.request, apiBaseUrl);
};

/**
 * Sets up workspace for test suites via a plain API request context.
 * This is needed for beforeAll hooks where the page fixture isn't available.
 */
export const setupWorkspaceForTests = async (): Promise<string> => {
  const requestContext = await request.newContext({ ignoreHTTPSErrors: true });
  try {
    return await fetchWorkspaceId(requestContext, getApiBaseUrl());
  } finally {
    await requestContext.dispose();
  }
};
