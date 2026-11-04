import { useMutation } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";

import { ConnectorCardValues } from "area/connector/components/ConnectorForm";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { ConnectorHelper, ConnectorT } from "core/domain/connector";

import { ApiCallOptions } from "../apiCall";
import { CommandErrorWithJobInfo, HttpError } from "../errors";
import {
  runCheckCommand,
  getCommandStatus,
  getCheckCommandOutput,
  cancelCommand,
  runConnectCommand,
  getConnectCommandOutput,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  GetCheckCommandOutput200,
  GetCheckCommandOutput200Status,
  JobConfigType,
  LogEvents,
  RunConnectCommandBody,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const commandsKeys = {
  all: [SCOPE_WORKSPACE, "commands"] as const,
  status: (commandId: string) => [...commandsKeys.all, "status", commandId] as const,
  output: (commandId: string) => [...commandsKeys.all, "output", commandId] as const,
};

const DOM_EXCEPTION_USER_CANCELLED = new DOMException("Aborted due to user cancellation", "AbortError");
const DOM_EXCEPTION_COMPONENT_UNMOUNTED = new DOMException("Aborting due to component unmount", "AbortError");

export function commandOutputHasFormattedLogs(
  output: GetCheckCommandOutput200
): output is GetCheckCommandOutput200 & { logs: { logType: "formatted"; logLines: string[] } } {
  return output.logs?.logType === "formatted";
}

export function commandOutputHasStructuredLogs(
  output: GetCheckCommandOutput200
): output is GetCheckCommandOutput200 & { logs: { logType: "structured"; logEvents: LogEvents } } {
  return output.logs?.logType === "structured";
}

const sleep = (milliseconds: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      return reject(signal.reason);
    }

    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, milliseconds);

    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(signal?.reason);
    };

    const cleanup = () => {
      signal?.removeEventListener("abort", onAbort);
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });

export const useCancelCommand = () => {
  const requestOptions = useRequestOptions();
  return useMutation(async (commandId: string) => cancelCommand({ id: commandId }, requestOptions));
};

export const COMMAND_POLLING_INTERVAL_MS = 500;

/**
 * Polls a command until it resolves (status is not "running" or "pending")
 * @returns The final status of the command or "cancelled" if it was cancelled
 */
export const pollCommandUntilResolved = async (
  commandId: string,
  requestOptions: ApiCallOptions,
  signal?: AbortSignal
): Promise<"completed" | "cancelled"> => {
  let commandResolved = false;

  while (!commandResolved) {
    const commandStatus = await getCommandStatus({ id: commandId }, { ...requestOptions, signal });
    const status = commandStatus.status;
    if (status === "pending" || status === "running") {
      await sleep(COMMAND_POLLING_INTERVAL_MS, signal);
    } else {
      commandResolved = true;
      // a command whose workload can't be found has no status; its output says how it ended
      return status ?? "completed";
    }
  }

  // This should never be reached, but TypeScript needs a return statement
  throw new Error("Command polling loop exited without a status");
};

export const useTestConnectorCommand = (
  props: {
    formType: "source" | "destination";
  } & (
    | { isEditMode: true; connector: ConnectorT }
    | {
        isEditMode?: false;
      }
  )
): {
  isTestConnectionInProgress: boolean;
  isSuccess: boolean;
  onStopTesting: () => void;
  testConnector: (v?: ConnectorCardValues, connectorOverride?: ConnectorT) => Promise<GetCheckCommandOutput200>;
  error: Error | null;
  reset: () => void;
} => {
  const { formatMessage } = useIntl();
  const requestOptions = useRequestOptions();
  const { mutateAsync: cancelCommand } = useCancelCommand();
  const commandIdRef = useRef<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const currentWorkspaceId = useCurrentWorkspaceId();

  // Automatically abort any ongoing request if the component using this hook unmounts
  useEffect(
    () => () => {
      abortControllerRef.current?.abort(DOM_EXCEPTION_COMPONENT_UNMOUNTED);
      if (commandIdRef.current) {
        cancelCommand(commandIdRef.current);
      }
    },
    [cancelCommand]
  );

  const mutation = useMutation(async ([values, connectorOverride]: [ConnectorCardValues | undefined, ConnectorT?]) => {
    try {
      abortControllerRef.current = new AbortController();
      const commandId = uuidv4();
      commandIdRef.current = commandId;

      const actor = connectorOverride ?? (props.isEditMode ? props.connector : undefined);
      const checkCommandPayload = actor
        ? {
            id: commandId,
            actor_id: ConnectorHelper.id(actor),
            config: connectorOverride ? undefined : values?.connectionConfiguration,
          }
        : {
            id: commandId,
            actor_definition_id: values?.serviceType,
            workspace_id: currentWorkspaceId,
            config: values?.connectionConfiguration,
          };

      await runCheckCommand(checkCommandPayload, { ...requestOptions, signal: abortControllerRef.current.signal });

      const status = await pollCommandUntilResolved(commandId, requestOptions, abortControllerRef.current.signal);

      if (status === "cancelled") {
        return {
          id: commandId,
          status: GetCheckCommandOutput200Status.failed,
        };
      }

      const output = await getCheckCommandOutput(
        { id: commandId, with_logs: true },
        { ...requestOptions, signal: abortControllerRef.current.signal }
      );

      if (output.status !== "succeeded") {
        throw new CommandErrorWithJobInfo(output.message || formatMessage({ id: "connector.check.failed" }), {
          logs: commandOutputHasFormattedLogs(output)
            ? output.logs
            : commandOutputHasStructuredLogs(output)
            ? output.logs.logEvents
            : undefined,
          id: commandId,
          configType:
            props.formType === "source"
              ? JobConfigType.check_connection_source
              : JobConfigType.check_connection_destination,
          failureReason: output.failureReason,
        });
      }

      return output;
    } catch (e) {
      if (e === DOM_EXCEPTION_COMPONENT_UNMOUNTED || e === DOM_EXCEPTION_USER_CANCELLED) {
        // This is a user-initiated reason for aborting, so we can ignore the error
        return {
          id: commandIdRef.current ?? "none",
          status: GetCheckCommandOutput200Status.failed,
        };
      }
      // Throw any other errors to be handled upstream
      throw e;
    } finally {
      commandIdRef.current = null;
      abortControllerRef.current = null;
    }
  });

  return {
    testConnector: (values, connectorOverride) => mutation.mutateAsync([values, connectorOverride]),
    isTestConnectionInProgress: mutation.isLoading,
    isSuccess: mutation.isSuccess && mutation.data?.status === GetCheckCommandOutput200Status.succeeded,
    error: mutation.error as Error | null,
    reset: mutation.reset,
    onStopTesting: () => {
      if (commandIdRef.current) {
        cancelCommand(commandIdRef.current);
      }
      abortControllerRef.current?.abort(DOM_EXCEPTION_USER_CANCELLED);
      mutation.reset();
    },
  };
};

/**
 * Runs the Connect command. Resolves with `false` when the server refuses the command with a 409. Only this request can
 * treat a 409 as a refusal, because the output endpoint also returns a 409 while the command runs.
 */
const tryRunConnectCommand = async (body: RunConnectCommandBody, options: ApiCallOptions) => {
  try {
    await runConnectCommand(body, options);
    return true;
  } catch (e) {
    if (e instanceof HttpError && e.status === 409) {
      return false;
    }
    throw e;
  }
};

/**
 * Tests whether the data plane of the workspace can open a TCP connection to a host and port, without a connector.
 * Resolves with the result of the probe, or with `undefined` when the server refuses to run the command with a 409, so
 * that the caller can run another check. Rejects when any other request fails, when the command was cancelled, or when
 * the component unmounts.
 */
export const useConnectCommand = () => {
  const { formatMessage } = useIntl();
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const abortControllerRef = useRef<AbortController | null>(null);

  // The probe ends on its own, so an unmount only stops the requests and does not cancel the command
  useEffect(() => () => abortControllerRef.current?.abort(DOM_EXCEPTION_COMPONENT_UNMOUNTED), []);

  const { mutateAsync } = useMutation(async ({ host, port }: Pick<RunConnectCommandBody, "host" | "port">) => {
    const abortController = new AbortController();
    abortControllerRef.current = abortController;
    const { signal } = abortController;
    const options = { ...requestOptions, signal };
    const commandId = uuidv4();

    const isAccepted = await tryRunConnectCommand({ id: commandId, workspace_id: workspaceId, host, port }, options);
    if (!isAccepted) {
      return undefined;
    }

    const status = await pollCommandUntilResolved(commandId, requestOptions, signal);
    if (status === "cancelled") {
      throw new Error(formatMessage({ id: "connector.check.failed" }));
    }

    return getConnectCommandOutput({ id: commandId }, options);
  });

  return mutateAsync;
};
