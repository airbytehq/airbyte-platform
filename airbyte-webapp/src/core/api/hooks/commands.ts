import { useMutation } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { ConnectorHelper, ConnectorT } from "core/domain/connector";
import { ConnectorCardValues } from "views/Connector/ConnectorForm";

import { CommandErrorWithJobInfo } from "../errors";
import { runCheckCommand, getCommandStatus, getCheckCommandOutput, cancelCommand } from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  GetCheckCommandOutput200,
  GetCheckCommandOutput200Status,
  JobConfigType,
  LogEvents,
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

const useCancelCommand = () => {
  const requestOptions = useRequestOptions();
  return useMutation(async (commandId: string) => cancelCommand({ id: commandId }, requestOptions));
};

const COMMAND_POLLING_INTERVAL_MS = 2000;

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
  testConnector: (v?: ConnectorCardValues) => Promise<GetCheckCommandOutput200>;
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

  const mutation = useMutation(async (values?: ConnectorCardValues) => {
    try {
      abortControllerRef.current = new AbortController();
      const commandId = uuidv4();
      commandIdRef.current = commandId;

      const checkCommandPayload = props.isEditMode
        ? {
            id: commandId,
            actor_id: ConnectorHelper.id(props.connector),
            config: values?.connectionConfiguration,
          }
        : {
            id: commandId,
            actor_definition_id: values?.serviceType,
            workspace_id: currentWorkspaceId,
            config: values?.connectionConfiguration,
          };

      await runCheckCommand(checkCommandPayload, { ...requestOptions, signal: abortControllerRef.current.signal });

      let cancelled = false;
      let commandResolved = false;

      while (!commandResolved) {
        const { status } = await getCommandStatus(
          { id: commandId },
          { ...requestOptions, signal: abortControllerRef.current.signal }
        );
        if (status === "pending" || status === "running") {
          await sleep(COMMAND_POLLING_INTERVAL_MS, abortControllerRef.current?.signal);
        } else {
          commandResolved = true;
          cancelled = status === "cancelled";
        }
      }

      if (cancelled) {
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
    testConnector: mutation.mutateAsync,
    isTestConnectionInProgress: mutation.isLoading,
    isSuccess: mutation.isSuccess,
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
