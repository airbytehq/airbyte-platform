import { expect, test } from "@playwright/test";
import { OrganizationApi, openAppAndGetOrganizationApi, setOnboardingStatus, signInOrRegister } from "helpers";

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

  const { workspaceId, organizationApi } = await openAppAndGetOrganizationApi(page);
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
