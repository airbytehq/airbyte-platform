import type { CheckOutput } from "../commands";
import type { FailureReason } from "@src/core/api/types/AirbyteClient";

/** Results of a connection check, for `outcomes.check` in a scenario or for `airbyte.checks.complete()`. */
export const checkOutput = {
  succeeded: (): CheckOutput => ({ status: "succeeded" }),
  /** A check that ran and found a problem. The webapp shows `message` under the connector form. */
  failed: ({ message, failureReason }: { message: string; failureReason?: FailureReason }): CheckOutput => ({
    status: "failed",
    message,
    ...(failureReason && { failureReason }),
  }),
};
