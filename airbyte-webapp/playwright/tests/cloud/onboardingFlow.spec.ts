import { Page, Request, expect, test } from "@playwright/test";
import { signInOrRegister } from "helpers";

const getWorkspaceId = (url: string) => {
  const workspaceId = new URL(url).pathname.match(/\/workspaces\/(?<workspaceId>[^/]+)/)?.groups?.workspaceId;
  if (!workspaceId) {
    throw new Error(`No workspace id in ${url}`);
  }
  return workspaceId;
};

interface OrganizationApi {
  origin: string;
  authorization: string;
  organizationId: string;
}

/** Reuses the app's auth header, and reads the organization from the workspace so a multi-organization user is safe. */
const getOrganizationApi = async (page: Page, appRequest: Request, workspaceId: string): Promise<OrganizationApi> => {
  const { authorization } = await appRequest.allHeaders();
  const { origin } = new URL(appRequest.url());
  const response = await page.request.post(`${origin}/api/v1/workspaces/get`, {
    headers: { authorization },
    data: { workspaceId },
  });
  expect(response.ok()).toBe(true);
  const { organizationId } = await response.json();
  return { origin, authorization, organizationId };
};

const setOnboardingStatus = async (page: Page, organizationApi: OrganizationApi, status: string) => {
  const { origin, authorization, organizationId } = organizationApi;
  const response = await page.request.post(`${origin}/api/v1/organizations/onboarding/update`, {
    headers: { authorization },
    data: { organizationId, status },
  });
  expect(response.ok()).toBe(true);
};

let cleanupOrganizationApi: OrganizationApi | undefined;

// Stage runs reuse one organization, so a failed run must not leave it in the flow.
test.afterEach(async ({ page }) => {
  if (cleanupOrganizationApi) {
    await setOnboardingStatus(page, cleanupOrganizationApi, "skipped").catch((error) =>
      console.warn("Onboarding cleanup failed, so the organization may still be in the flow", error)
    );
  }
});

test("a new organization can enter the onboarding flow and skip it", async ({ page }) => {
  await signInOrRegister(page);

  const appRequestPromise = page.waitForRequest((request) =>
    request.url().includes("/api/v1/organizations/get_organization_info")
  );
  await page.goto("./");
  const appRequest = await appRequestPromise;
  await expect(page).toHaveURL(/\/workspaces\/[^/]+/);
  const workspaceId = getWorkspaceId(page.url());
  const organizationApi = await getOrganizationApi(page, appRequest, workspaceId);
  cleanupOrganizationApi = organizationApi;
  const flowUrl = new RegExp(`/workspaces/${workspaceId}/onboarding-flow`);
  const expectNormalApp = async () => {
    await expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/connections`));
    await expect(page).toHaveTitle(/Connections/);
  };

  await test.step("the app sends an organization with a step status into the flow", async () => {
    await setOnboardingStatus(page, organizationApi, "survey");
    await page.goto(`./workspaces/${workspaceId}/connections`);
    await expect(page).toHaveURL(flowUrl);
  });

  await test.step("Skip opens the normal app", async () => {
    await page.getByRole("button", { name: "Skip onboarding" }).click();
    await expectNormalApp();
  });

  await test.step("the app no longer sends the organization into the flow", async () => {
    await page.reload();
    await expectNormalApp();
    await page.goto(`./workspaces/${workspaceId}/onboarding-flow`);
    await expectNormalApp();
  });
});
