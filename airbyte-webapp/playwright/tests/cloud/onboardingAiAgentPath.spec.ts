import { Page, expect, test } from "@playwright/test";
import { ConnectorIds } from "@src/area/connector/utils/constants";
import { DestinationReadList, SourceReadList } from "@src/core/api/types/AirbyteClient";
import {
  OrganizationApi,
  openAppAndGetOrganizationApi,
  postConfigApi,
  setOnboardingStatus,
  signInOrRegister,
} from "helpers";

interface Cleanup {
  organizationApi: OrganizationApi;
  sourceId?: string;
  destinationId?: string;
}

let cleanup: Cleanup | undefined;

// Stage runs reuse one organization, so a failed run must not leave it in the flow or leave its drafts behind.
test.afterEach(async ({ page }) => {
  if (!cleanup) {
    return;
  }
  const { organizationApi, sourceId, destinationId } = cleanup;
  const warn = (error: unknown) => console.warn("Onboarding cleanup failed", error);
  await setOnboardingStatus(page, organizationApi, "skipped").catch(warn);
  if (sourceId) {
    await postConfigApi(page, organizationApi, "sources/delete", { sourceId }).catch(warn);
  }
  if (destinationId) {
    await postConfigApi(page, organizationApi, "destinations/delete", { destinationId }).catch(warn);
  }
});

/** The drafts are the saved source and destination picks, so a draft left by an earlier run would change the prompt. */
const deleteLeftoverDrafts = async (page: Page, organizationApi: OrganizationApi, workspaceId: string) => {
  const { sources }: SourceReadList = await postConfigApi(page, organizationApi, "sources/list", { workspaceId });
  const { destinations }: DestinationReadList = await postConfigApi(page, organizationApi, "destinations/list", {
    workspaceId,
  });
  await Promise.all([
    ...sources
      .filter(({ isDraft }) => isDraft)
      .map(({ sourceId }) => postConfigApi(page, organizationApi, "sources/delete", { sourceId })),
    ...destinations
      .filter(({ isDraft }) => isDraft)
      .map(({ destinationId }) => postConfigApi(page, organizationApi, "destinations/delete", { destinationId })),
  ]);
};

const createDraft = async (
  page: Page,
  organizationApi: OrganizationApi,
  path: "sources/create" | "destinations/create",
  data: object
) => postConfigApi(page, organizationApi, path, { ...data, connectionConfiguration: {}, createAsDraft: true });

test("a new organization can connect its AI agent with the MCP install screen", async ({ page, context }) => {
  await signInOrRegister(page);
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);

  const { workspaceId, organizationApi } = await openAppAndGetOrganizationApi(page);
  const testCleanup: Cleanup = { organizationApi };
  cleanup = testCleanup;
  const getStatus = async () =>
    (
      await postConfigApi(page, organizationApi, "organizations/get_organization_info", {
        organizationId: organizationApi.organizationId,
      })
    ).onboardingStatus;
  const installHeading = page.getByRole("heading", { level: 1, name: "Install MCP" });
  const expectNormalApp = async () => {
    await expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/connections`));
    await expect(installHeading).toBeHidden();
  };

  await test.step("the organization starts at the path choice with a File source and a BigQuery destination", async () => {
    await deleteLeftoverDrafts(page, organizationApi, workspaceId);
    const { sourceId } = await createDraft(page, organizationApi, "sources/create", {
      workspaceId,
      sourceDefinitionId: ConnectorIds.Sources.File,
      name: "File",
    });
    testCleanup.sourceId = sourceId;
    const { destinationId } = await createDraft(page, organizationApi, "destinations/create", {
      workspaceId,
      destinationDefinitionId: ConnectorIds.Destinations.BigQuery,
      name: "BigQuery",
    });
    testCleanup.destinationId = destinationId;
    await setOnboardingStatus(page, organizationApi, "choose_method");
    await page.goto(`./workspaces/${workspaceId}/onboarding-flow`);
    await expect(page.getByRole("heading", { name: "Where do you want to work?" })).toBeVisible();
  });

  await test.step("My AI agent completes onboarding and opens the MCP install screen in the flow", async () => {
    await page.getByText("My AI agent").click();
    await page.getByRole("button", { name: "Continue", exact: true }).click();
    await expect(installHeading).toBeVisible();
    await expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/onboarding-flow`));
    await expect.poll(getStatus).toBe("completed");
  });

  await test.step("each assistant tab shows its instruction for the server URL", async () => {
    await expect.soft(page.getByText(/^In Claude Desktop, open connector settings/)).toBeVisible();
    await page.getByRole("button", { name: "Cursor", exact: true }).click();
    await expect.soft(page.getByText(/^In Cursor, open MCP settings/)).toBeVisible();
    await expect.soft(page.getByText("https://mcp.airbyte.com/mcp", { exact: true })).toBeVisible();
  });

  await test.step("the first prompt names the saved source and destination, and Copy copies it", async () => {
    const prompt = page.getByText(/^Set up a connection from File.* to BigQuery, pick a few core streams/);
    await expect(prompt).toBeVisible();
    await expect.soft(page.getByText(/^Personalized from your saved File.*BigQuery choices\.$/)).toBeVisible();
    const promptText = await prompt.innerText();
    await page.getByRole("button", { name: "Copy prompt" }).click();
    await expect
      .configure({ soft: true })
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(promptText);
  });

  await test.step("Finished opens the normal app", async () => {
    await page.getByRole("link", { name: "Finished" }).click();
    await expectNormalApp();
  });

  await test.step("the MCP install screen shows once", async () => {
    await page.goto(`./workspaces/${workspaceId}/onboarding-flow`);
    await expectNormalApp();
  });
});
