/**
 * Connection Timeline tests. They run against the mock backend in playwright/mock.
 */
import { connectionUI, jobUI } from "../../helpers/connection";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

/** A PokeAPI → E2E Testing connection that has never synced. No stream is selected, as in a new connection. */
const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: false }),
});

test.describe(() => {
  test.use({ airbyteScenario: scenario({ connections: [pokeConnection] }) });

  test("cancel a sync and open its logs from the timeline", async ({ page, context, airbyte }) => {
    // The test reads the links that the "copy link" buttons write to the clipboard.
    await context.grantPermissions(["clipboard-read", "clipboard-write"]);
    const { connectionId } = airbyte.store.connections.get(pokeConnection);
    // By default, the mock ends a sync the first time the page asks for its status. hold() keeps the sync running, so
    // the page shows "Sync running" and the user can cancel the sync.
    airbyte.jobs.hold();

    await test.step("open the Timeline tab", async () => {
      await page.goto(airbyte.paths.connection(pokeConnection, "timeline"));
    });

    await test.step("the timeline has no events", async () => {
      await expect.soft(page.getByText("No events to display")).toBeVisible();
    });

    await test.step("start a sync", async () => {
      await connectionUI.startManualSync(page);

      await connectionUI.verifySyncStatus.running(page);
    });

    await test.step("the webapp asks the server to sync the connection", async () => {
      expect.soft(airbyte.calls("syncConnection")).toEqual([{ connectionId }]);
    });

    await test.step("cancel the sync", async () => {
      await connectionUI.cancelSync(page);

      await connectionUI.verifySyncStatus.cancelled(page);
    });

    await test.step("the webapp asks the server to cancel the sync job", async () => {
      expect.soft(airbyte.calls("cancelJob")).toEqual([{ id: airbyte.jobs.latest().job.id }]);
    });

    await test.step("open the copied link to the cancelled sync event", async () => {
      await jobUI.openEventMenu(page);
      const eventLink = await jobUI.copyEventLink(page);
      await page.goto(eventLink);
    });

    await test.step("the job logs modal opens", async () => {
      await jobUI.verifyLogsModal(page);
      expect.soft(airbyte.calls.last("getJobInfoWithoutLogs")).toEqual({ id: airbyte.jobs.latest().job.id });
    });

    await test.step("open the logs from the event menu", async () => {
      await jobUI.closeLogsModal(page);
      await page.goto(airbyte.paths.connection(pokeConnection, "timeline"));
      await jobUI.openEventMenu(page);
      await jobUI.openLogsFromMenu(page);
    });

    await test.step("open the copied link to the attempt", async () => {
      const attemptLink = await jobUI.copyAttemptLink(page);
      await jobUI.closeLogsModal(page);
      await page.goto(attemptLink);
    });

    await test.step("the job logs modal opens from the attempt link", async () => {
      await jobUI.verifyLogsModal(page);
    });
  });
});
