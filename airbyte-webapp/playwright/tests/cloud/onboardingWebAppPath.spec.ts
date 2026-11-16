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

const PUBLIC_CSV_URL =
  "https://raw.githubusercontent.com/datasets/country-codes/a6129fd4a004685c51432ddc04e3543ff750bef7/data/country-codes.csv";
const DATASET_NAME = "countries";
// A connection check or a schema discovery starts a job, which can take minutes on a cold environment.
const JOB_TIMEOUT = 240_000;

interface Cleanup {
  organizationApi: OrganizationApi;
  sourceId?: string;
  destinationId?: string;
}

let cleanup: Cleanup | undefined;

/**
 * Stage runs reuse one organization. A ready source or destination left behind would become the connector that the
 * next run's steps open, so the cleanup deletes them, and the source deletion deletes the connection.
 */
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

/** A File source or End-to-End Testing destination left by an earlier failed run would change what the steps open. */
const deleteLeftoverConnectors = async (page: Page, organizationApi: OrganizationApi, workspaceId: string) => {
  const { sources }: SourceReadList = await postConfigApi(page, organizationApi, "sources/list", { workspaceId });
  const { destinations }: DestinationReadList = await postConfigApi(page, organizationApi, "destinations/list", {
    workspaceId,
  });
  const leftoverSources = sources.filter(({ sourceDefinitionId }) => sourceDefinitionId === ConnectorIds.Sources.File);
  const leftoverDestinations = destinations.filter(
    ({ destinationDefinitionId }) => destinationDefinitionId === ConnectorIds.Destinations.EndToEndTesting
  );
  await Promise.all([
    ...leftoverSources.map(({ sourceId }) => postConfigApi(page, organizationApi, "sources/delete", { sourceId })),
    ...leftoverDestinations.map(({ destinationId }) =>
      postConfigApi(page, organizationApi, "destinations/delete", { destinationId })
    ),
  ]);
};

/** The connector passes its connection check without credentials. */
const createDraftSource = async (page: Page, organizationApi: OrganizationApi, workspaceId: string) => {
  const { sourceId } = await postConfigApi(page, organizationApi, "sources/create", {
    workspaceId,
    sourceDefinitionId: ConnectorIds.Sources.File,
    name: "File",
    connectionConfiguration: {},
    createAsDraft: true,
  });
  return sourceId as string;
};

/** The connector passes its connection check without credentials. */
const createDraftDestination = async (page: Page, organizationApi: OrganizationApi, workspaceId: string) => {
  const { destinationId } = await postConfigApi(page, organizationApi, "destinations/create", {
    workspaceId,
    destinationDefinitionId: ConnectorIds.Destinations.EndToEndTesting,
    name: "End-to-End Testing",
    connectionConfiguration: {},
    createAsDraft: true,
  });
  return destinationId as string;
};

test("a new organization can create its first connection and run the first sync in the web app", async ({ page }) => {
  test.setTimeout(15 * 60_000);
  await signInOrRegister(page);

  const { workspaceId, organizationApi } = await openAppAndGetOrganizationApi(page);
  const testCleanup: Cleanup = { organizationApi };
  cleanup = testCleanup;
  const flowButton = (name: string) => page.getByRole("button", { name, exact: true });
  // The documentation panel of a connector step has a heading with the connector name too.
  const connectorHeading = (name: RegExp) => page.getByRole("heading", { level: 1, name }).first();
  const createConnectionHeading = page.getByRole("heading", { level: 1, name: "Create a connection" });
  const readyToSync = page.getByRole("heading", { name: "Ready to run your first sync" });
  const getStatus = async () =>
    (
      await postConfigApi(page, organizationApi, "organizations/get_organization_info", {
        organizationId: organizationApi.organizationId,
      })
    ).onboardingStatus;

  const sourceId =
    await test.step("the organization starts at the source step with a File source draft and an End-to-End Testing destination draft", async () => {
      await deleteLeftoverConnectors(page, organizationApi, workspaceId);
      const draftSourceId = await createDraftSource(page, organizationApi, workspaceId);
      testCleanup.sourceId = draftSourceId;
      const draftDestinationId = await createDraftDestination(page, organizationApi, workspaceId);
      testCleanup.destinationId = draftDestinationId;
      await setOnboardingStatus(page, organizationApi, "create_source");
      await page.goto(`./workspaces/${workspaceId}/onboarding-flow`);
      await expect(connectorHeading(/File/)).toBeVisible();
      return draftSourceId;
    });

  await test.step("set up the File source with a public CSV file", async () => {
    await page.locator('input[name="connectionConfiguration.dataset_name"]').fill(DATASET_NAME);
    await page.getByTestId("connectionConfiguration.provider-listbox-button").click();
    await page.getByRole("option", { name: /HTTPS/ }).click();
    await page.locator('input[name="connectionConfiguration.url"]').fill(PUBLIC_CSV_URL);
    await flowButton("Continue").click();
    await expect(connectorHeading(/End-to-End Testing/)).toBeVisible({ timeout: JOB_TIMEOUT });
  });

  await test.step("set up the End-to-End Testing destination", async () => {
    await page.getByRole("button", { name: "Select a value" }).click();
    await page.getByRole("option").first().click();
    await flowButton("Continue").click();
    await expect(createConnectionHeading).toBeVisible({ timeout: JOB_TIMEOUT });
  });

  await test.step("the stream table shows the discovered stream", async () => {
    await expect(page.getByTestId("sync-catalog-table")).toBeVisible({ timeout: JOB_TIMEOUT });
    await expect.soft(page.getByTestId("sync-catalog-table").getByText(DATASET_NAME, { exact: true })).toBeVisible();
  });

  await test.step("select Full refresh | Overwrite, because the CSV file has no primary key", async () => {
    await page
      .getByTestId("sync-catalog-table")
      .getByRole("button", { name: "Full refresh | Overwrite + Deduped" })
      .click();
    await page
      .getByTestId("sync-mode-select-listbox-options")
      .getByRole("option", { name: "Full refresh | Overwrite", exact: true })
      .click();
  });

  await test.step("create the connection", async () => {
    await flowButton("Continue").click();
    await expect(readyToSync).toBeVisible({ timeout: 60_000 });
    await expect.poll(getStatus).toBe("run_first_sync");
  });

  const connectionId = await test.step("the connection has a manual schedule, so no sync started", async () => {
    const { connections } = await postConfigApi(page, organizationApi, "web_backend/connections/list", {
      workspaceId,
      sourceId: [sourceId],
    });
    expect.soft(connections).toHaveLength(1);
    expect.soft(connections[0]?.scheduleType).toBe("manual");
    expect.soft(connections[0]?.latestSyncJobStatus).toBeUndefined();
    return connections[0]?.connectionId as string;
  });

  await test.step("a reload opens Run your first sync again", async () => {
    await page.reload();
    await expect(readyToSync).toBeVisible();
  });

  await test.step("Back opens the connection step, which keeps the created connection", async () => {
    await flowButton("Back").click();
    await expect(createConnectionHeading).toBeVisible();
    await expect.soft(page.getByText("Your connection is ready")).toBeVisible();
    await expect.poll(getStatus).toBe("create_connection");
    await flowButton("Continue").click();
    await expect(readyToSync).toBeVisible();
  });

  await test.step("run the first sync", async () => {
    await flowButton("Run your first sync").click();
    await expect(page.getByRole("heading", { name: /First sync (running|ended)/ })).toBeVisible({ timeout: 60_000 });
    await expect.poll(getStatus).toBe("completed");
    await expect.soft(page.getByRole("link", { name: "Install MCP" })).toBeVisible();
    await expect.soft(page.getByRole("link", { name: "Add sources" })).toBeVisible();
    await expect.soft(page.getByRole("link", { name: "Add destinations" })).toBeVisible();
  });

  await test.step("Monitor sync opens the connection page in the normal app", async () => {
    await page.getByRole("link", { name: "Monitor sync" }).click();
    await expect(page).toHaveURL(new RegExp(`/workspaces/${workspaceId}/connections/${connectionId}`));
  });
});
