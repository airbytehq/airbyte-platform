import type { Body } from "./routes";
import type {
  AirbyteCatalog,
  GetCheckCommandOutput200,
  GetCommandStatus200Status,
  GetDiscoverCommandOutput200,
} from "@src/core/api/types/AirbyteClient";

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

/** The result of a schema discovery, as the webapp reads it with getDiscoverCommandOutput. */
export type DiscoverOutput = Pick<GetDiscoverCommandOutput200, "status" | "catalog" | "catalogId">;

/** A schema discovery of a source. The webapp sends runDiscoverCommand, then getCommandStatus until it is done. */
export interface DiscoverCommand {
  /** The webapp chooses the ID of a command. */
  id: string;
  request: Body<"runDiscoverCommand">;
  status: GetCommandStatus200Status;
  output: DiscoverOutput;
}

interface CommandsOptions {
  /** The result of a check that the test does not end itself. Default: the check succeeds. */
  checkResult?: CheckOutput;
  /** The catalog that every schema discovery finds. Without it, a discovery fails the test. */
  discoveredCatalog?: AirbyteCatalog;
  /**
   * The ID of the discovered catalog. The server saves a catalog once, and gives a later discovery that finds the same
   * catalog the same ID (CatalogServiceJooqImpl.kt). Every discovery in the mock finds the same catalog, so all of them
   * have this ID.
   */
  discoveredCatalogId: string;
}

/**
 * The commands the webapp started in a test: connection checks and schema discoveries.
 *
 * A check finishes the first time the webapp reads its status, with the scenario result, unless the test holds it
 * (lifecycle.ts).
 *
 * A discovery finishes as soon as the webapp starts it, with the scenario catalog. A cancel that follows then changes
 * nothing, as on the server, where a cancel of an ended command does not change it (CommandService.kt). In dev builds,
 * React StrictMode runs the effects of the discovery page twice, and the first cleanup cancels the discovery right
 * after it starts (useDiscoverSchemaQuery in sources.tsx). A discovery that is still running at that moment would end
 * as cancelled, and the page would show an error that production builds never show.
 */
export function createCommands({
  checkResult = checkOutput.succeeded(),
  discoveredCatalog,
  discoveredCatalogId,
}: CommandsOptions) {
  const checks: CheckCommand[] = [];
  // The IDs of the checks that the test holds. They are kept apart, so the stored checks are plain API objects.
  const heldCheckIds = new Set<string>();
  let holdChecks = false;
  const discoveries: DiscoverCommand[] = [];

  function findCheck(id: string) {
    return checks.find((check) => check.id === id);
  }

  function findDiscovery(id: string) {
    return discoveries.find((discovery) => discovery.id === id);
  }

  function statusOf(id: string) {
    const command = findCheck(id) ?? findDiscovery(id);
    return command && snapshot({ id: command.id, status: command.status });
  }

  /** The command ID is an idempotency key: a second request with the same ID returns the same command. */
  function startCheck(request: Body<"runCheckCommand">) {
    if (!findCheck(request.id)) {
      checks.push({ id: request.id, request: structuredClone(request), status: "running" });
      if (holdChecks) {
        heldCheckIds.add(request.id);
      }
    }
    return snapshot(findCheck(request.id)!);
  }

  /** Throws when the scenario declares no catalog, because the mock cannot know what the source would find. */
  function startDiscovery(request: Body<"runDiscoverCommand">) {
    if (!discoveredCatalog) {
      throw new Error(
        `The webapp discovered the schema of source ${request.actor_id}, but the scenario declares no ` +
          "outcomes.discover catalog"
      );
    }
    if (!findDiscovery(request.id)) {
      discoveries.push({
        id: request.id,
        request: structuredClone(request),
        status: "completed",
        output: { status: "succeeded", catalog: structuredClone(discoveredCatalog), catalogId: discoveredCatalogId },
      });
    }
    return snapshot(findDiscovery(request.id)!);
  }

  /** The status the webapp polls, for a check or a discovery. A check that is not held finishes on its first read. */
  function readStatus(id: string) {
    const check = findCheck(id);
    if (check?.status === "running" && !heldCheckIds.has(id)) {
      finish(check, "completed", checkResult);
    }
    return statusOf(id);
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
    startDiscovery,
    readStatus,
    findCheck: (id: string) => {
      const check = findCheck(id);
      return check && snapshot(check);
    },
    findDiscovery: (id: string) => {
      const discovery = findDiscovery(id);
      return discovery && snapshot(discovery);
    },
    /** Cancels a command the webapp no longer needs, for example when the user leaves the page. */
    cancel: (id: string) => {
      const check = findCheck(id);
      if (check?.status === "running") {
        finish(check, "cancelled");
      }
      return statusOf(id);
    },
    hold: () => {
      holdChecks = true;
    },
    all: () => checks.map(snapshot),
    finishHeld,
  };
}

export type Commands = ReturnType<typeof createCommands>;
