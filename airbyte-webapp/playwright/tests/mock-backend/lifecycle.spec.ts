/**
 * Self-tests of the fixed test time (playwright/mock/clock.ts) and of long-running operations
 * (playwright/mock/lifecycle.ts). The browser page stands in for the webapp: it sends the same requests, and it
 * polls with setTimeout the way the webapp does.
 */
import type { CheckOutput } from "../../mock";
import type { Page } from "@playwright/test";

import { test, expect } from "@playwright/test";

import { installBackend, post } from "./selfTest";
import {
  MOCK_START_TIME,
  catalogs,
  checkOutput,
  connection,
  defs,
  destination,
  ids,
  scenario,
  source,
} from "../../mock";

const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});
const connectionScenario = scenario({ connections: [pokeConnection] });
const connectionId = ids.connection(1);
const sourceId = ids.source(1);

/**
 * Runs a check of the scenario source the way the webapp does (core/api/hooks/commands.ts): start it, poll its
 * status every 2 seconds while it runs, then read the output. The promise resolves with the output, or "cancelled".
 */
function runCheck(page: Page, id: string) {
  return page.evaluate(
    async ({ id, sourceId }) => {
      const post = (path: string, body: object) =>
        fetch(`/api/v1/commands/${path}`, { method: "POST", body: JSON.stringify(body) }).then((response) =>
          response.json()
        );
      const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

      await post("run/check", { id, actor_id: sourceId });
      let { status } = await post("status", { id });
      while (status === "pending" || status === "running") {
        await sleep(2000);
        ({ status } = await post("status", { id }));
      }
      return status === "cancelled" ? "cancelled" : ((await post("output/check", { id })) as CheckOutput);
    },
    { id, sourceId }
  );
}

/**
 * Runs a sync the way the connection pages see it (core/api/hooks/connections.tsx): start it, then poll the
 * connection status every 10 seconds while it runs. The promise resolves with the last connection status.
 */
function runSync(page: Page, connectionId: string) {
  return page.evaluate(async (connectionId) => {
    const post = (path: string, body: object) =>
      fetch(`/api/v1/connections/${path}`, { method: "POST", body: JSON.stringify(body) }).then((response) =>
        response.json()
      );
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    await post("sync", { connectionId });
    let [{ connectionSyncStatus }] = await post("status", { connectionIds: [connectionId] });
    while (connectionSyncStatus === "running") {
      await sleep(10000);
      [{ connectionSyncStatus }] = await post("status", { connectionIds: [connectionId] });
    }
    return connectionSyncStatus as string;
  }, connectionId);
}

/** The type and user of each timeline event of the connection, newest first. */
async function timeline(page: Page): Promise<Array<{ eventType: string; user?: string }>> {
  const { events } = (await post(page, "connections/events/list", { connectionId })).body;
  return events.map(({ eventType, user }: { eventType: string; user?: { email: string } }) => ({
    eventType,
    user: user?.email,
  }));
}

test.describe("Mock backend lifecycles", () => {
  // These tests use no deployed backend, so they do not use the signed-in state of the setup project.
  test.use({ storageState: { cookies: [], origins: [] } });

  test("starts the browser clock at the fixed start time, then lets it run", async ({ context, page, baseURL }) => {
    await installBackend(context, page, baseURL, connectionScenario);

    const first = await page.evaluate(() => Date.now());

    expect(first).toBeGreaterThanOrEqual(MOCK_START_TIME.getTime());
    expect(first).toBeLessThan(MOCK_START_TIME.getTime() + 60_000);
    await expect.poll(() => page.evaluate(() => Date.now())).toBeGreaterThan(first);
  });

  test("finishes a check on its first status read, with the result the scenario declares", async ({
    context,
    page,
    baseURL,
  }) => {
    const failing = connectionScenario.with({
      outcomes: { check: checkOutput.failed({ message: "Bad credentials" }) },
    });
    const backend = await installBackend(context, page, baseURL, failing);

    const output = await runCheck(page, "check-1");

    expect(output).toEqual({ id: "check-1", status: "failed", message: "Bad credentials" });
    expect(backend.airbyte.calls.count("getCommandStatus")).toBe(1);
    expect(backend.problems()).toEqual([]);
  });

  test("finishes a check with success when the scenario declares no result", async ({ context, page, baseURL }) => {
    await installBackend(context, page, baseURL, connectionScenario);

    expect(await runCheck(page, "check-1")).toEqual({ id: "check-1", status: "succeeded" });
  });

  test("holds a check as running until the test fails it, then waits for the next status poll", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, connectionScenario);
    const { airbyte } = backend;
    airbyte.checks.hold();

    const output = runCheck(page, "check-1");
    // The browser clock keeps running, so the page can poll again by itself at any time. The poll counts in this
    // file are therefore lower bounds, not exact numbers.
    await expect.poll(() => airbyte.calls.count("getCommandStatus")).toBeGreaterThanOrEqual(1);
    expect(airbyte.checks.latest()).toMatchObject({ id: "check-1", status: "running" });
    const pollsBeforeFail = airbyte.calls.count("getCommandStatus");

    const failed = await airbyte.checks.fail("Bad credentials");

    // fail() returns after the webapp has polled again, so that poll is already recorded.
    expect(airbyte.calls.count("getCommandStatus")).toBeGreaterThan(pollsBeforeFail);
    expect(failed).toMatchObject({ id: "check-1", status: "completed", output: { status: "failed" } });
    expect(await output).toEqual({ id: "check-1", status: "failed", message: "Bad credentials" });
    expect(backend.problems()).toEqual([]);
  });

  test("ends a held check with complete() or cancel()", async ({ context, page, baseURL }) => {
    const { airbyte } = await installBackend(context, page, baseURL, connectionScenario);
    airbyte.checks.hold();

    const completed = runCheck(page, "check-1");
    await expect.poll(() => airbyte.checks.all().length).toBe(1);
    await airbyte.checks.complete();
    expect(await completed).toEqual({ id: "check-1", status: "succeeded" });

    const cancelled = runCheck(page, "check-2");
    await expect.poll(() => airbyte.checks.all().length).toBe(2);
    await airbyte.checks.cancel();
    expect(await cancelled).toBe("cancelled");
    expect(airbyte.checks.all().map(({ id, status }) => ({ id, status }))).toEqual([
      { id: "check-1", status: "completed" },
      { id: "check-2", status: "cancelled" },
    ]);
  });

  test("rejects lifecycle calls when no held check is running", async ({ context, page, baseURL }) => {
    const { airbyte } = await installBackend(context, page, baseURL, connectionScenario);

    await expect(airbyte.checks.fail("Bad credentials")).rejects.toThrow(
      "No held check is running. Call airbyte.checks.hold() before the webapp starts the check."
    );
    expect(() => airbyte.checks.latest()).toThrow("The webapp started no check in this test");
  });

  test("ends a sync on its first status read, with the result the scenario declares and a matching event", async ({
    context,
    page,
    baseURL,
  }) => {
    const failing = connectionScenario.with({
      outcomes: { sync: { status: "failed", failureReason: { externalMessage: "Source is down" } } },
    });
    const backend = await installBackend(context, page, baseURL, failing);

    expect(await runSync(page, connectionId)).toBe("failed");
    const { job, attempts } = (await post(page, "jobs/get_without_logs", { id: 1 })).body;
    const [status] = (await post(page, "connections/status", { connectionIds: [connectionId] })).body;

    expect(job).toMatchObject({ id: 1, status: "failed", configId: connectionId });
    expect(attempts).toEqual([{ attempt: expect.objectContaining({ id: 0, status: "failed" }) }]);
    // The status page shows the failure reason of the connection status.
    expect(status).toMatchObject({
      connectionSyncStatus: "failed",
      failureReason: { externalMessage: "Source is down" },
    });
    expect(await timeline(page)).toEqual([
      { eventType: "SYNC_FAILED", user: undefined },
      { eventType: "SYNC_STARTED", user: "test@example.com" },
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("holds a sync as running until the test completes it, then waits for the next status poll", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, connectionScenario);
    const { airbyte } = backend;
    airbyte.jobs.hold();

    const status = runSync(page, connectionId);
    await expect.poll(() => airbyte.calls.count("getConnectionStatuses")).toBeGreaterThanOrEqual(1);
    expect(airbyte.jobs.latest().job.status).toBe("running");
    expect((await post(page, "connections/sync_progress", { connectionId })).body).toMatchObject({
      jobId: 1,
      syncStartedAt: airbyte.jobs.latest().job.createdAt,
    });
    const pollsBeforeComplete = airbyte.calls.count("getConnectionStatuses");

    const completed = await airbyte.jobs.complete();

    // complete() returns after the page has polled again, so that poll is already recorded.
    expect(airbyte.calls.count("getConnectionStatuses")).toBeGreaterThan(pollsBeforeComplete);
    expect(completed.job.status).toBe("succeeded");
    expect(await status).toBe("synced");
    expect(await timeline(page)).toEqual([
      { eventType: "SYNC_SUCCEEDED", user: undefined },
      { eventType: "SYNC_STARTED", user: "test@example.com" },
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("ends a held sync with fail() or cancel()", async ({ context, page, baseURL }) => {
    const { airbyte } = await installBackend(context, page, baseURL, connectionScenario);
    airbyte.jobs.hold();

    const failed = runSync(page, connectionId);
    await expect.poll(() => airbyte.jobs.all().length).toBe(1);
    await airbyte.jobs.fail({ externalMessage: "Source is down" });
    expect(await failed).toBe("failed");

    // fail() moved the browser clock forward by 10 seconds. The mock must add the same time to the timestamps it
    // writes, so the start time of the next job matches the browser time. The check allows 2 seconds for slow runs.
    const browserSeconds = Math.floor((await page.evaluate(() => Date.now())) / 1000);
    const cancelled = runSync(page, connectionId);
    await expect.poll(() => airbyte.jobs.all().length).toBe(2);
    expect(Math.abs(airbyte.jobs.latest().job.createdAt - browserSeconds)).toBeLessThanOrEqual(2);
    await airbyte.jobs.cancel();
    expect(await cancelled).toBe("incomplete");
    expect(airbyte.jobs.all().map(({ job }) => job.status)).toEqual(["failed", "cancelled"]);
    expect((await timeline(page)).map(({ eventType }) => eventType)).toEqual([
      "SYNC_CANCELLED",
      "SYNC_STARTED",
      "SYNC_FAILED",
      "SYNC_STARTED",
    ]);
  });

  test("ends a sync that the user cancels on the page, with the user in the event", async ({
    context,
    page,
    baseURL,
  }) => {
    const { airbyte } = await installBackend(context, page, baseURL, connectionScenario);
    airbyte.jobs.hold();

    await post(page, "connections/sync", { connectionId });
    const { job } = (await post(page, "jobs/cancel", { id: 1 })).body;

    expect(job.status).toBe("cancelled");
    expect(await timeline(page)).toEqual([
      { eventType: "SYNC_CANCELLED", user: "test@example.com" },
      { eventType: "SYNC_STARTED", user: "test@example.com" },
    ]);
    await expect(airbyte.jobs.cancel()).rejects.toThrow("No held job is running");
  });
});
