/**
 * Connection Status tests. They run against the mock backend in playwright/mock.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 */
import type { Page } from "@playwright/test";

import { connectionUI } from "../../helpers/connection";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

/** A PokeAPI → E2E Testing connection that has never synced. Its pokemon stream is selected. */
const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

/** The status indicator at the top of the Status tab. Its `data-status` attribute is the status of the connection. */
const statusIndicator = (page: Page) => page.getByTestId("connection-status-indicator");
const syncButton = (page: Page) => page.getByTestId("manual-sync-button");
const cancelButton = (page: Page) => page.getByTestId("cancel-sync-button");

test.describe(() => {
  test.use({ airbyteScenario: scenario({ connections: [pokeConnection] }) });

  test("start and cancel a sync on the Status tab", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(pokeConnection);
    // By default, the mock ends a sync the first time the page asks for its status. hold() keeps the sync running, so
    // the page shows the running sync and the user can cancel it.
    airbyte.jobs.hold();

    await test.step("open the Status tab", async () => {
      await page.goto(airbyte.paths.connection(pokeConnection, "status"));
    });

    await test.step("the connection is pending, because it has never synced", async () => {
      await expect.soft(statusIndicator(page)).toHaveAttribute("data-status", "pending");
      await expect.soft(syncButton(page)).toBeEnabled();
    });

    await test.step("the webapp asks the server for the status of the connection", async () => {
      expect.soft(airbyte.calls("getConnectionStatuses")).toContainEqual({ connectionIds: [connectionId] });
    });

    await test.step("start a sync", async () => {
      await connectionUI.startManualSync(page);

      await expect(statusIndicator(page)).toHaveAttribute("data-status", "running");
    });

    await test.step("the sync is starting, and only the cancel button shows", async () => {
      await expect.soft(statusIndicator(page)).toHaveAttribute("data-loading", "true");
      await expect.soft(page.getByTestId("streams-list-subtitle")).toContainText("Sync starting");
      await expect.soft(syncButton(page)).toBeHidden();
      await expect.soft(cancelButton(page)).toBeEnabled();
    });

    await test.step("the webapp asks the server to sync the connection once", async () => {
      expect.soft(airbyte.calls("syncConnection")).toEqual([{ connectionId }]);
    });

    await test.step("cancel the sync", async () => {
      await connectionUI.cancelSync(page);

      await expect(statusIndicator(page)).toHaveAttribute("data-status", "incomplete");
    });

    await test.step("the sync button shows again", async () => {
      await expect.soft(syncButton(page)).toBeEnabled();
    });

    await test.step("the webapp asks the server to cancel the sync job", async () => {
      expect.soft(airbyte.calls("cancelJob")).toEqual([{ id: airbyte.jobs.latest().job.id }]);
    });

    await test.step("reload the page", async () => {
      await page.reload();
    });

    await test.step("the connection stays incomplete, and the sync button shows", async () => {
      await expect.soft(statusIndicator(page)).toHaveAttribute("data-status", "incomplete");
      await expect.soft(syncButton(page)).toBeEnabled();
      await expect.soft(cancelButton(page)).toBeHidden();
    });
  });
});
