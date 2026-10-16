import type { Body } from "./routes";
import type { GetCheckCommandOutput200, GetCommandStatus200Status } from "@src/core/api/types/AirbyteClient";

import { checkOutput } from "./builders/commands";
import { snapshot } from "./store";

/** The result of a check, as the webapp reads it with getCheckCommandOutput. The webapp chooses the ID. */
export type CheckOutput = Omit<GetCheckCommandOutput200, "id">;

/** A connection check. The webapp sends runCheckCommand, then getCommandStatus until the check is done. */
export interface CheckCommand {
  /** The webapp chooses the ID of a command. */
  id: string;
  request: Body<"runCheckCommand">;
  status: GetCommandStatus200Status;
  /** Set when the check is done. */
  output?: CheckOutput;
}

/**
 * The commands the webapp started in a test. Today the mock knows one kind of command: the connection check.
 *
 * A check finishes the first time the webapp reads its status, with the scenario result, unless the test holds it
 * (lifecycle.ts).
 */
export function createCommands(defaultCheckOutput: CheckOutput = checkOutput.succeeded()) {
  const checks: CheckCommand[] = [];
  // The IDs of the checks that the test holds. They are kept apart, so the stored checks are plain API objects.
  const heldCheckIds = new Set<string>();
  let holdChecks = false;

  function find(id: string) {
    return checks.find((check) => check.id === id);
  }

  /** The command ID is an idempotency key: a second request with the same ID returns the same command. */
  function startCheck(request: Body<"runCheckCommand">) {
    if (!find(request.id)) {
      checks.push({ id: request.id, request: structuredClone(request), status: "running" });
      if (holdChecks) {
        heldCheckIds.add(request.id);
      }
    }
    return snapshot(find(request.id)!);
  }

  /** The status the webapp polls. A check that is not held finishes on its first status read. */
  function readStatus(id: string) {
    const check = find(id);
    if (check?.status === "running" && !heldCheckIds.has(id)) {
      finish(check, "completed", defaultCheckOutput);
    }
    return check && snapshot(check);
  }

  function finish(check: CheckCommand, status: "completed" | "cancelled", output?: CheckOutput) {
    check.status = status;
    check.output = output && structuredClone(output);
  }

  /** Ends the latest held check that is still running. Throws when there is none. */
  function finishHeld(status: "completed" | "cancelled", output?: CheckOutput) {
    const check = checks.findLast(({ id, status }) => heldCheckIds.has(id) && status === "running");
    if (!check) {
      throw new Error("No held check is running. Call airbyte.checks.hold() before the webapp starts the check.");
    }
    finish(check, status, output);
    return snapshot(check);
  }

  return {
    startCheck,
    readStatus,
    find: (id: string) => {
      const check = find(id);
      return check && snapshot(check);
    },
    /** Cancels a command the webapp no longer needs, for example when the user leaves the page. */
    cancel: (id: string) => {
      const check = find(id);
      if (check?.status === "running") {
        finish(check, "cancelled");
      }
      return check && snapshot(check);
    },
    hold: () => {
      holdChecks = true;
    },
    all: () => checks.map(snapshot),
    finishHeld,
  };
}

export type Commands = ReturnType<typeof createCommands>;
