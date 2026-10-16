/**
 * Self-test of the timeline events that the mock writes for sync jobs (playwright/mock/jobs.ts and
 * playwright/mock/builders/events.ts).
 *
 * The API types the summary of an event as any JSON, so TypeScript cannot check it. The webapp checks each event
 * against a zod schema (eventTypeToSchemaMap in src/pages/connections/ConnectionTimelinePage/types.ts) and does not
 * show an event that fails it. It shows no error either. So this test checks that the timeline shows every event
 * that the mock writes.
 */
import { post } from "./selfTest";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

test.describe("Mock backend timeline events", () => {
  test.use({ airbyteScenario: scenario({ connections: [pokeConnection] }) });

  test("writes an event for each sync start and end, and the timeline shows each event", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(pokeConnection);
    await page.goto(airbyte.paths.connection(pokeConnection, "timeline"));
    await expect(page.getByText("No events to display")).toBeVisible();

    // Job 1 succeeds on its first read, as the scenario declares no other outcome.
    await post(page, "connections/sync", { connectionId });
    await post(page, "connections/status", { connectionIds: [connectionId] });
    // Job 2 is cancelled by the user. Job 3 fails.
    airbyte.jobs.hold();
    await post(page, "connections/sync", { connectionId });
    await post(page, "jobs/cancel", { id: 2 });
    await post(page, "connections/sync", { connectionId });
    await airbyte.jobs.fail({ failureType: "system_error", externalMessage: "Source is down" });
    await page.reload();

    await test.step("the timeline shows every event", async () => {
      await expect.soft(page.getByText("manually started a sync")).toHaveCount(3);
      await expect.soft(page.getByText("Sync succeeded")).toHaveCount(1);
      await expect.soft(page.getByText("Sync cancelled")).toHaveCount(1);
      await expect.soft(page.getByText("cancelled a sync")).toHaveCount(1);
      await expect.soft(page.getByText("Sync failed")).toHaveCount(1);
      await expect.soft(page.getByText("Source is down")).toBeVisible();
    });
    expect(airbyte.jobs.all().map(({ job }) => job.status)).toEqual(["succeeded", "cancelled", "failed"]);
  });
});
