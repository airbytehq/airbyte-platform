import type { MockClock } from "./clock";
import type { CheckCommand, CheckOutput, Commands } from "./commands";
import type { Jobs, SyncEnd, SyncFailure } from "./jobs";
import type { Op } from "./routes";
import type { JobInfoRead } from "@src/core/api/types/AirbyteClient";

import { checkOutput } from "./builders/commands";

/**
 * A long-running operation, such as a connection check or a sync, takes several requests to finish: the webapp
 * starts it, then asks for its status until it is done, then reads the result.
 *
 * By default, an operation finishes the first time the webapp asks for its status, with the result that the
 * scenario declares (`outcomes` in scenario.ts). No test waits for it.
 *
 * To see the operation while it runs, a test calls `hold()` before the webapp starts it. The operation then stays
 * running until the test calls `complete()`, `fail()` or `cancel()`.
 *
 * Each of `complete()`, `fail()` and `cancel()` ends the latest held operation. Then it moves the browser clock
 * forward by the poll interval of the webapp, so that the webapp asks for the status again. The method returns when
 * the mock has received that status request. The page may not show the new status yet, because the browser has not
 * read the response. Check the page with an assertion that retries, such as `expect(locator).toBeVisible()`.
 */
export interface Lifecycle<Operation, Result, Failure> {
  /** Operations that start after this call keep running until the test completes, fails or cancels them. */
  hold(): void;
  /** The operations the webapp started in this test, oldest first. */
  all(): ReadonlyArray<Readonly<Operation>>;
  /** The operation the webapp started last. Throws when the webapp started none. */
  latest(): Readonly<Operation>;
  complete(result?: Result): Promise<Readonly<Operation>>;
  fail(failure: Failure): Promise<Readonly<Operation>>;
  cancel(): Promise<Readonly<Operation>>;
}

export interface LifecycleTools {
  clock: MockClock;
  /** Resolves when the webapp next sends a request for the operation. */
  nextCall(op: Op): Promise<void>;
}

/** How often the webapp polls the status of a command (COMMAND_POLLING_INTERVAL_MS in core/api/hooks/commands.ts). */
const COMMAND_POLL_INTERVAL_MS = 2000;

/**
 * Moves the browser clock past the next poll of the webapp, and waits until the webapp has asked for the status.
 * The wait fails after 10 seconds, so a page that stopped polling fails the test with a clear message.
 */
async function waitForPoll({ clock, nextCall }: LifecycleTools, statusOp: Op, intervalMs: number) {
  const polled = nextCall(statusOp);
  await clock.fastForward(intervalMs);

  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`The webapp did not send ${statusOp} after the operation ended`)), 10000);
  });
  await Promise.race([polled, timeout]).finally(() => clearTimeout(timer));
}

/**
 * The connection checks of a test. `fail(message)` is a check that ran and found a problem: the command completes
 * and its output has the status "failed". This is what a user sees when a connector cannot connect.
 * `cancel()` is a command that stopped before it finished.
 */
export type CheckLifecycle = Lifecycle<CheckCommand, CheckOutput, string>;

export function checkLifecycle(commands: Commands, tools: LifecycleTools): CheckLifecycle {
  async function end(status: "completed" | "cancelled", output?: CheckOutput) {
    const check = commands.finishHeld(status, output);
    await waitForPoll(tools, "getCommandStatus", COMMAND_POLL_INTERVAL_MS);
    return check;
  }

  return {
    hold: commands.hold,
    all: commands.all,
    latest: () => {
      const latest = commands.all().at(-1);
      if (!latest) {
        throw new Error("The webapp started no check in this test");
      }
      return latest;
    },
    complete: (result = checkOutput.succeeded()) => end("completed", result),
    fail: (message) => end("completed", checkOutput.failed({ message })),
    cancel: () => end("cancelled"),
  };
}

/** How often the webapp polls the status of a connection (CONNECTION_STATUS_REFETCH_INTERVAL in connections.tsx). */
const CONNECTION_STATUS_POLL_INTERVAL_MS = 10_000;

/**
 * The sync jobs of a test. Ending a job also writes its timeline event (jobs.ts). `complete()` takes no result.
 * `cancel()` here is a cancel without a user, for example by the scheduler. A cancel from the page goes through
 * the cancelJob request.
 */
export type JobLifecycle = Lifecycle<JobInfoRead, never, SyncFailure>;

export function jobLifecycle(jobs: Jobs, tools: LifecycleTools): JobLifecycle {
  async function end(result: SyncEnd) {
    const jobInfo = jobs.endHeld(result);
    await waitForPoll(tools, "getConnectionStatuses", CONNECTION_STATUS_POLL_INTERVAL_MS);
    return jobInfo;
  }

  return {
    hold: jobs.hold,
    all: jobs.all,
    latest: () => {
      const latest = jobs.all().at(-1);
      if (!latest) {
        throw new Error("The webapp started no job in this test");
      }
      return latest;
    },
    complete: () => end({ status: "succeeded" }),
    fail: (failureReason) => end({ status: "failed", failureReason }),
    cancel: () => end({ status: "cancelled" }),
  };
}
