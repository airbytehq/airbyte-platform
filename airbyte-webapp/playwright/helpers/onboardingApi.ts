import { Page, Request, expect } from "@playwright/test";

const getWorkspaceId = (url: string) => {
  const workspaceId = new URL(url).pathname.match(/\/workspaces\/(?<workspaceId>[^/]+)/)?.groups?.workspaceId;
  if (!workspaceId) {
    throw new Error(`No workspace id in ${url}`);
  }
  return workspaceId;
};

interface ConfigApi {
  origin: string;
  authorization: string;
}

export interface OrganizationApi extends ConfigApi {
  organizationId: string;
}

/**
 * Posts to the Config API of the environment with the app's auth header, and fails the test on an error status. A
 * delete answers with no body.
 */
export const postConfigApi = async (page: Page, api: ConfigApi, path: string, data: object) => {
  const response = await page.request.post(`${api.origin}/api/v1/${path}`, {
    headers: { authorization: api.authorization },
    data,
  });
  const body = await response.text();
  expect(response.ok(), `POST ${path} returned ${response.status()}: ${body}`).toBe(true);
  return body ? JSON.parse(body) : undefined;
};

/** Reuses the app's auth header, and reads the organization from the workspace so a multi-organization user is safe. */
const getOrganizationApi = async (page: Page, appRequest: Request, workspaceId: string): Promise<OrganizationApi> => {
  const { authorization } = await appRequest.allHeaders();
  const { origin } = new URL(appRequest.url());
  const { organizationId } = await postConfigApi(page, { origin, authorization }, "workspaces/get", { workspaceId });
  return { origin, authorization, organizationId };
};

export const setOnboardingStatus = async (page: Page, api: OrganizationApi, status: string) => {
  await postConfigApi(page, api, "organizations/onboarding/update", { organizationId: api.organizationId, status });
};

/**
 * Opens the app and reads the workspace and the organization API from the first organization info request, which
 * carries the app's auth header.
 */
export const openAppAndGetOrganizationApi = async (page: Page) => {
  const appRequestPromise = page.waitForRequest((request) =>
    request.url().includes("/api/v1/organizations/get_organization_info")
  );
  await page.goto("./");
  const appRequest = await appRequestPromise;
  await expect(page).toHaveURL(/\/workspaces\/[^/]+/);
  const workspaceId = getWorkspaceId(page.url());
  const organizationApi = await getOrganizationApi(page, appRequest, workspaceId);
  return { workspaceId, organizationApi };
};
