import { useEffect, useRef } from "react";

import { ConnectorCheckParams, useCurrentWorkspace, useCheckConnector } from "core/api";
import { CheckConnectionRead } from "core/api/types/AirbyteClient";
import { ConnectorHelper } from "core/domain/connector";
import { ConnectorT } from "core/domain/connector/types";

import { ConnectorCardValues } from "../ConnectorForm";

export const useTestConnector = (
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
  testConnector: (v?: ConnectorCardValues) => Promise<CheckConnectionRead>;
  error: Error | null;
  reset: () => void;
} => {
  const { mutateAsync, isLoading, error, isSuccess, reset } = useCheckConnector(props.formType);
  const workspace = useCurrentWorkspace();

  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(
    () => () => {
      abortControllerRef.current?.abort();
    },
    []
  );

  return {
    isTestConnectionInProgress: isLoading,
    isSuccess,
    error,
    reset,
    onStopTesting: () => {
      abortControllerRef.current?.abort();
      reset();
    },
    testConnector: async (values) => {
      const controller = new AbortController();

      abortControllerRef.current = controller;

      let payload: ConnectorCheckParams | null = null;

      if (props.isEditMode) {
        // When we are editing current connector
        if (values) {
          payload = {
            type: "modify",
            connectionConfiguration: values.connectionConfiguration,
            name: values.name,
            connectorId: ConnectorHelper.id(props.connector),
            signal: controller.signal,
          };
        } else {
          // just testing current connection
          payload = {
            type: "retest",
            connectorId: ConnectorHelper.id(props.connector),
            signal: controller.signal,
          };
        }
      } else if (values) {
        // creating new connection
        payload = {
          type: "create",
          connectionConfiguration: values.connectionConfiguration,
          signal: controller.signal,
          connectorDefinitionId: values.serviceType,
          workspaceId: workspace.workspaceId,
        };
      }

      if (!payload) {
        console.error("Unexpected state met: no connectorId or connectorDefinitionId provided");

        throw new Error("Unexpected state met");
      }

      return mutateAsync(payload);
    },
  };
};
