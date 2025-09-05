import { Page } from "@playwright/test";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000000";

export const getWorkspaceId = async (page: Page): Promise<string> => {
  const serverHost = process.env.AIRBYTE_SERVER_HOST;
  const apiBaseUrl = serverHost ? `${serverHost}/api/v1` : `${page.url().split("/").slice(0, 3).join("/")}/api/v1`;

  const response = await page.request.post(`${apiBaseUrl}/workspaces/list_by_organization_id`, {
    data: {
      organizationId: DEFAULT_ORGANIZATION_ID,
      pagination: { pageSize: 1, rowOffset: 0 },
    },
  });

  const { workspaces } = await response.json();
  return workspaces[0].workspaceId;
};
