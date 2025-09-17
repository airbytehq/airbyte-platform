import { Page, chromium } from "@playwright/test";

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

  if (!response.ok()) {
    throw new Error("Failed to get workspace ID");
  }

  const { workspaces } = await response.json();
  if (!workspaces || workspaces.length === 0) {
    throw new Error("No workspaces found");
  }

  return workspaces[0].workspaceId;
};

/**
 * Sets up workspace for test suites by creating a manual browser context
 * This is needed for beforeAll hooks where page fixture isn't available
 */
export const setupWorkspaceForTests = async (): Promise<string> => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await context.newPage();

  await page.goto("https://localhost:3000");
  const workspaceId = await getWorkspaceId(page);

  await context.close();
  await browser.close();

  return workspaceId;
};
