import { useMutation } from "@tanstack/react-query";

import { ConnectionConfiguration } from "area/connector/types";
import { LogsRequestError } from "core/api";

import {
  checkConnectionToDestination,
  checkConnectionToDestinationForUpdate,
  checkConnectionToSource,
  checkConnectionToSourceForUpdate,
  executeDestinationCheckConnection,
  executeSourceCheckConnection,
} from "../generated/AirbyteClient";
import { CheckConnectionRead, CheckConnectionReadStatus } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

interface BaseParams {
  signal: AbortSignal;
}

interface CreateConnectorParams extends BaseParams {
  type: "create";
  connectorDefinitionId: string;
  workspaceId: string;
  connectionConfiguration: ConnectionConfiguration;
}

interface RetestConnectorParams extends BaseParams {
  type: "retest";
  connectorId: string;
}

interface ModifyConnectorParams extends BaseParams {
  type: "modify";
  name: string;
  connectorId: string;
  connectionConfiguration: ConnectionConfiguration;
}

export type ConnectorCheckParams = CreateConnectorParams | RetestConnectorParams | ModifyConnectorParams;

export const useCheckConnector = (type: "source" | "destination") => {
  const requestOptions = useRequestOptions();
  return useMutation<CheckConnectionRead, Error, ConnectorCheckParams>(async (params) => {
    const options = { ...requestOptions, signal: params.signal };
    let result: CheckConnectionRead;
    switch (params.type) {
      case "create":
        // Test the connection for a new (yet unsaved) connector
        if (type === "source") {
          result = await executeSourceCheckConnection(
            {
              connectionConfiguration: params.connectionConfiguration,
              workspaceId: params.workspaceId,
              sourceDefinitionId: params.connectorDefinitionId,
            },
            options
          );
        } else {
          result = await executeDestinationCheckConnection(
            {
              connectionConfiguration: params.connectionConfiguration,
              workspaceId: params.workspaceId,
              destinationDefinitionId: params.connectorDefinitionId,
            },
            options
          );
        }
        break;

      case "retest":
        // Retest an existing connector without any changes to its config
        if (type === "source") {
          result = await checkConnectionToSource({ sourceId: params.connectorId }, options);
        } else {
          result = await checkConnectionToDestination({ destinationId: params.connectorId }, options);
        }
        break;

      case "modify":
        // Test an existing connector with new properties, but will reuse existing stored secrets
        if (type === "source") {
          result = await checkConnectionToSourceForUpdate(
            {
              name: params.name,
              sourceId: params.connectorId,
              connectionConfiguration: params.connectionConfiguration,
            },
            options
          );
        } else {
          result = await checkConnectionToDestinationForUpdate(
            {
              name: params.name,
              destinationId: params.connectorId,
              connectionConfiguration: params.connectionConfiguration,
            },
            options
          );
        }
    }

    if (!result.jobInfo?.succeeded) {
      throw new LogsRequestError(result.jobInfo, "Failed to run connection tests.");
    } else if (result.status === CheckConnectionReadStatus.failed) {
      throw new LogsRequestError(result.jobInfo, result.message);
    }

    return result;
  });
};
