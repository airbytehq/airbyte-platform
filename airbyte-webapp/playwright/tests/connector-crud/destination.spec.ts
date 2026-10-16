/**
 * Destination tests: create, edit and delete a destination.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 */
import type { Page } from "@playwright/test";

import { fillFormField, navigateToConnectorCreation, selectConnectorFromMarketplace } from "../../helpers/ui";
import { defs, destination, expect, scenario, test } from "../../mock";

const { destinationDefinitionId } = defs.e2eTestingDestination.input.definition;

/** An End-to-End Testing configuration that logs the first `maxEntryCount` entries of each stream. */
const configWithMaxEntries = (maxEntryCount: number) => ({
  test_destination: {
    test_destination_type: "LOGGING",
    logging_config: { logging_type: "FirstN", max_entry_count: maxEntryCount },
  },
});

/** The configuration that the End-to-End Testing form saves when the user changes no field. */
const defaultConfig = configWithMaxEntries(100);

const maxEntryCountInput = (page: Page) =>
  page.locator("input[name='connectionConfiguration.test_destination.logging_config.max_entry_count']");

/** The ID of the destination whose page is open, from the URL. */
function destinationIdInUrl(page: Page) {
  return page.url().match(/\/destination\/([a-f0-9-]{36})/)?.[1];
}

const submitButton = (page: Page) => page.locator("button[type='submit']");

test.describe("New destination", () => {
  test.describe(() => {
    // An empty workspace with End-to-End Testing in the marketplace. Every connection check succeeds.
    test.use({ airbyteScenario: scenario({ destinationDefinitions: [defs.e2eTestingDestination] }) });

    test("create the first destination of a workspace", async ({ page, airbyte }) => {
      await test.step("open the destinations page", async () => {
        await page.goto(`/workspaces/${airbyte.workspaceId}/destination`);
      });

      await test.step("the new destination page opens, because the workspace has no destination", async () => {
        await expect(page).toHaveURL(/\/destination\/new-destination$/);
        await expect.soft(page.locator("h2")).toContainText("Set up a new destination");
      });

      await test.step("select End-to-End Testing, and fill in the name", async () => {
        await selectConnectorFromMarketplace(page, "end", "End-to-End Testing");
        await fillFormField(page, "input[name='name']", "E2E Testing destination");
      });

      await test.step("submit the form", async () => {
        await submitButton(page).click();
        await expect(page).toHaveURL(/\/destination\/[a-f0-9-]{36}\/connections$/);
      });

      await test.step("the webapp saved a draft destination, checked it, and opened its page", async () => {
        const destinationId = destinationIdInUrl(page);
        expect.soft(airbyte.calls("createDestination")).toEqual([
          {
            name: "E2E Testing destination",
            destinationDefinitionId,
            workspaceId: airbyte.workspaceId,
            connectionConfiguration: defaultConfig,
            createAsDraft: true,
          },
        ]);
        expect.soft(airbyte.calls("runCheckCommand")).toEqual([{ id: expect.any(String), actor_id: destinationId }]);
      });
    });
  });

  test.describe(() => {
    // An empty workspace with End-to-End Testing in the marketplace, and agent-assisted setup turned on.
    test.use({
      airbyteScenario: scenario({
        destinationDefinitions: [defs.e2eTestingDestination],
        experiments: { "connector.agentAssistedSetup": true },
      }),
    });

    test("set up a destination with the form when agent-assisted setup is on", async ({ page, airbyte }) => {
      await test.step("select End-to-End Testing on the new destination page", async () => {
        await navigateToConnectorCreation(page, "destination", airbyte.workspaceId);
        await selectConnectorFromMarketplace(page, "end", "End-to-End Testing");
      });

      await test.step("the form shows, and the setup assistant does not", async () => {
        // "Agent" is the label of the Agent/Form toggle. The toggle shows only when agent-assisted setup is on.
        await expect.soft(page.getByText("Agent", { exact: true })).toBeVisible();
        await expect.soft(page.locator("input[name='name']")).toBeVisible();
        await expect.soft(page.getByText("Connector Setup Assistant")).toBeHidden();
      });
    });
  });
});

test.describe("Existing destination", () => {
  // An End-to-End Testing destination that logs the first 100 entries, without a connection. Every connection check
  // succeeds.
  const e2eDestination = destination({
    def: defs.e2eTestingDestination,
    name: "E2E Testing destination",
    config: defaultConfig,
  });
  test.use({ airbyteScenario: scenario({ destinations: [e2eDestination] }) });

  /** Opens the destinations page and clicks the destination in the list. */
  async function openDestination(page: Page, workspaceId: string) {
    await page.goto(`/workspaces/${workspaceId}/destination`);
    await page.getByTestId("destinationsTable").getByRole("link", { name: "E2E Testing destination" }).first().click();
    await expect(maxEntryCountInput(page)).toHaveValue("100");
  }

  test("edit a destination twice without leaving the page", async ({ page, airbyte }) => {
    const { destinationId } = airbyte.store.destinations.get(e2eDestination);

    await test.step("open the destination from the destinations page", async () => {
      await openDestination(page, airbyte.workspaceId);
    });

    await test.step("set the number of entries to 10 and save", async () => {
      await maxEntryCountInput(page).fill("10");
      await submitButton(page).click();
      await expect(page.locator("[data-id='success-result']")).toBeVisible();
    });

    await test.step("the destination logs 10 entries, and the save button is disabled", async () => {
      await expect.soft(maxEntryCountInput(page)).toHaveValue("10");
      await expect.soft(submitButton(page)).toBeDisabled();
    });

    await test.step("the webapp checked the configuration with 10 entries, then saved it", async () => {
      expect
        .soft(airbyte.calls("runCheckCommand"))
        .toEqual([{ id: expect.any(String), actor_id: destinationId, config: configWithMaxEntries(10) }]);
      expect
        .soft(airbyte.calls("updateDestination"))
        .toEqual([
          { destinationId, name: "E2E Testing destination", connectionConfiguration: configWithMaxEntries(10) },
        ]);
    });

    await test.step("set the number of entries to 20 and save again", async () => {
      await maxEntryCountInput(page).fill("20");
      await expect(submitButton(page)).toBeEnabled();
      await submitButton(page).click();
      // The success message of the first save can still show, so wait for the second save request instead.
      await expect.poll(() => airbyte.calls("updateDestination")).toHaveLength(2);
    });

    await test.step("the destination logs 20 entries, and the save button is disabled", async () => {
      await expect.soft(maxEntryCountInput(page)).toHaveValue("20");
      await expect.soft(submitButton(page)).toBeDisabled();
    });

    await test.step("the webapp checked the configuration with 20 entries, then saved it", async () => {
      expect
        .soft(airbyte.calls.last("runCheckCommand"))
        .toEqual({ id: expect.any(String), actor_id: destinationId, config: configWithMaxEntries(20) });
      expect.soft(airbyte.calls.last("updateDestination")).toEqual({
        destinationId,
        name: "E2E Testing destination",
        connectionConfiguration: configWithMaxEntries(20),
      });
    });
  });

  test("delete a destination", async ({ page, airbyte }) => {
    const { destinationId } = airbyte.store.destinations.get(e2eDestination);

    await test.step("open the destination from the destinations page", async () => {
      await openDestination(page, airbyte.workspaceId);
    });

    await test.step("delete the destination, and type its name to confirm", async () => {
      await page.locator("button[data-id='open-delete-modal']").click();
      await page.locator("input[id='confirmation-text']").fill("E2E Testing destination");
      await page.locator("button[data-id='delete']").click();
    });

    await test.step("the new destination page opens, because no destination is left", async () => {
      await expect.soft(page).toHaveURL(/\/destination\/new-destination$/);
    });

    await test.step("the webapp deleted the destination", async () => {
      expect.soft(airbyte.calls("deleteDestination")).toEqual([{ destinationId }]);
    });
  });
});
