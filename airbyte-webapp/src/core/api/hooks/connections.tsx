import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { SyncSchema } from "core/domain/catalog";
import { getFrequencyFromScheduleData } from "core/services/analytics";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { SCOPE_WORKSPACE } from "services/Scope";

import { useCurrentWorkspace, useInvalidateWorkspaceStateQuery } from "./workspaces";
import { useAppMonitoringService } from "../../../hooks/services/AppMonitoringService";
import { useNotificationService } from "../../../hooks/services/Notification";
import {
  createOrUpdateStateSafe,
  deleteConnection,
  getState,
  getStateType,
  resetConnection,
  resetConnectionStream,
  syncConnection,
  webBackendCreateConnection,
  webBackendGetConnection,
  webBackendListConnectionsForWorkspace,
  webBackendUpdateConnection,
} from "../generated/AirbyteClient";
import {
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionStateCreateOrUpdate,
  ConnectionStream,
  DestinationRead,
  NamespaceDefinitionType,
  OperationCreate,
  SourceDefinitionRead,
  SourceRead,
  WebBackendConnectionListItem,
  WebBackendConnectionListRequestBody,
  WebBackendConnectionRead,
  WebBackendConnectionReadList,
  WebBackendConnectionRequestBody,
  WebBackendConnectionUpdate,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const connectionsKeys = {
  all: [SCOPE_WORKSPACE, "connections"] as const,
  lists: (sourceOrDestinationIds: string[] = []) => [...connectionsKeys.all, "list", ...sourceOrDestinationIds],
  detail: (connectionId: string) => [...connectionsKeys.all, "details", connectionId] as const,
  getState: (connectionId: string) => [...connectionsKeys.all, "getState", connectionId] as const,
};

export interface ConnectionValues {
  name?: string;
  scheduleType: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  prefix?: string;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
  /**
   * TOD0: change to AirbyteCatalog after migration on react-hook form since we don't use custom id field anymore
   * https://github.com/airbytehq/airbyte-platform-internal/issues/8639
   */
  syncCatalog: SyncSchema;
  operations?: OperationCreate[];
}

interface CreateConnectionProps {
  values: ConnectionValues;
  source: SourceRead;
  destination: DestinationRead;
  sourceDefinition?: Pick<SourceDefinitionRead, "sourceDefinitionId">;
  destinationDefinition?: { name: string; destinationDefinitionId: string };
  sourceCatalogId: string | undefined;
}

export const useSyncConnection = () => {
  const requestOptions = useRequestOptions();
  const { trackError } = useAppMonitoringService();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const { registerNotification } = useNotificationService();
  const workspaceId = useCurrentWorkspaceId();
  const { formatMessage } = useIntl();

  return useMutation(
    (connection: WebBackendConnectionRead | WebBackendConnectionListItem) => {
      analyticsService.track(Namespace.CONNECTION, Action.SYNC, {
        actionDescription: "Manual triggered sync",
        connector_source: connection.source?.sourceName,
        connector_source_definition_id: connection.source?.sourceDefinitionId,
        connector_destination: connection.destination?.destinationName,
        connector_destination_definition_id: connection.destination?.destinationDefinitionId,
        frequency: getFrequencyFromScheduleData(connection.scheduleData),
      });

      return syncConnection({ connectionId: connection.connectionId }, requestOptions);
    },
    {
      onError: (error: Error) => {
        trackError(error);
        registerNotification({
          id: `tables.startSyncError.${error.message}`,
          text: `${formatMessage({ id: "connection.startSyncError" })}: ${error.message}`,
          type: "error",
        });
      },
      onSuccess: async () => {
        await webBackendListConnectionsForWorkspace({ workspaceId }, requestOptions).then((updatedConnections) =>
          queryClient.setQueryData(connectionsKeys.lists(), updatedConnections)
        );
      },
    }
  );
};

export const useResetConnection = () => {
  const requestOptions = useRequestOptions();
  const mutation = useMutation(["useResetConnection"], (connectionId: string) =>
    resetConnection({ connectionId }, requestOptions)
  );
  const activeMutationsCount = useIsMutating(["useResetConnection"]);
  return { ...mutation, isLoading: activeMutationsCount > 0 };
};

export const useResetConnectionStream = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  return useMutation((streams: ConnectionStream[]) => resetConnectionStream({ connectionId, streams }, requestOptions));
};

export const useGetConnectionQuery = () => {
  const requestOptions = useRequestOptions();
  return useMutation((request: WebBackendConnectionRequestBody) => webBackendGetConnection(request, requestOptions))
    .mutateAsync;
};

export const useGetConnection = (
  connectionId: string,
  options?: { refetchInterval: number }
): WebBackendConnectionRead => {
  const getConnectionQuery = useGetConnectionQuery();

  return useSuspenseQuery(
    connectionsKeys.detail(connectionId),
    () => getConnectionQuery({ connectionId, withRefreshedCatalog: false }),
    options
  );
};

export const useCreateConnection = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const invalidateWorkspaceSummary = useInvalidateWorkspaceStateQuery();

  return useMutation(
    async ({
      values,
      source,
      destination,
      sourceDefinition,
      destinationDefinition,
      sourceCatalogId,
    }: CreateConnectionProps) => {
      const response = await webBackendCreateConnection(
        {
          sourceId: source.sourceId,
          destinationId: destination.destinationId,
          ...values,
          status: "active",
          sourceCatalogId,
        },
        requestOptions
      );

      const enabledStreams = values.syncCatalog.streams
        .map((stream) => stream.config?.selected && stream.stream?.name)
        .filter(Boolean);

      analyticsService.track(Namespace.CONNECTION, Action.CREATE, {
        actionDescription: "New connection created",
        frequency: getFrequencyFromScheduleData(values.scheduleData),
        connector_source_definition: source?.sourceName,
        connector_source_definition_id: sourceDefinition?.sourceDefinitionId,
        connector_destination_definition: destination?.destinationName,
        connector_destination_definition_id: destinationDefinition?.destinationDefinitionId,
        available_streams: values.syncCatalog.streams.length,
        enabled_streams: enabledStreams.length,
        enabled_streams_list: JSON.stringify(enabledStreams),
      });

      return response;
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData<WebBackendConnectionReadList>(connectionsKeys.lists(), (lst) => ({
          connections: [data, ...(lst?.connections ?? [])],
        }));
        invalidateWorkspaceSummary();
      },
    }
  );
};

export const useDeleteConnection = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();

  return useMutation(
    (connection: WebBackendConnectionRead) =>
      deleteConnection({ connectionId: connection.connectionId }, requestOptions),
    {
      onSuccess: (_data, connection) => {
        analyticsService.track(Namespace.CONNECTION, Action.DELETE, {
          actionDescription: "Connection deleted",
          connector_source: connection.source?.sourceName,
          connector_source_definition_id: connection.source?.sourceDefinitionId,
          connector_destination: connection.destination?.destinationName,
          connector_destination_definition_id: connection.destination?.destinationDefinitionId,
        });

        queryClient.removeQueries(connectionsKeys.detail(connection.connectionId));
        queryClient.setQueryData<WebBackendConnectionReadList>(connectionsKeys.lists(), (lst) => ({
          connections: lst?.connections.filter((conn) => conn.connectionId !== connection.connectionId) ?? [],
        }));
      },
    }
  );
};

export const useUpdateConnection = () => {
  const { registerNotification } = useNotificationService();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (connectionUpdate: WebBackendConnectionUpdate) => webBackendUpdateConnection(connectionUpdate, requestOptions),
    {
      onSuccess: (updatedConnection) => {
        queryClient.setQueryData(connectionsKeys.detail(updatedConnection.connectionId), updatedConnection);
        // Update the connection inside the connections list response
        queryClient.setQueryData<WebBackendConnectionReadList>(connectionsKeys.lists(), (ls) => ({
          ...ls,
          connections:
            ls?.connections.map((conn) => {
              if (conn.connectionId === updatedConnection.connectionId) {
                return updatedConnection;
              }
              return conn;
            }) ?? [],
        }));
      },
      onError: (error: Error) => {
        // If there is not user-facing message in the API response, we should fall back to a generic message
        const fallbackKey = error.message && error.message === "common.error" ? "connection.updateFailed" : undefined;
        registerNotification({
          id: "update-connection-error",
          type: "error",
          text: fallbackKey ? <FormattedMessage id={fallbackKey} /> : error.message,
        });
      },
    }
  );
};

export const useRemoveConnectionsFromList = (): ((connectionIds: string[]) => void) => {
  const queryClient = useQueryClient();

  return useCallback(
    (connectionIds: string[]) => {
      queryClient.setQueryData<WebBackendConnectionReadList>(connectionsKeys.lists(), (ls) => ({
        ...ls,
        connections: ls?.connections.filter((c) => !connectionIds.includes(c.connectionId)) ?? [],
      }));
    },
    [queryClient]
  );
};

export const getConnectionListQueryKey = (connectorIds?: string[]) => {
  return connectionsKeys.lists(connectorIds);
};

export const useConnectionListQuery = (
  workspaceId: string,
  sourceId?: string[],
  destinationId?: string[]
): (() => Promise<ConnectionListTransformed>) => {
  const requestOptions = useRequestOptions();

  return async () => {
    const { connections } = await webBackendListConnectionsForWorkspace(
      { workspaceId, sourceId, destinationId },
      requestOptions
    );
    const connectionsByConnectorId = new Map<string, WebBackendConnectionListItem[]>();
    connections.forEach((connection) => {
      connectionsByConnectorId.set(connection.source.sourceId, [
        ...(connectionsByConnectorId.get(connection.source.sourceId) || []),
        connection,
      ]);
      connectionsByConnectorId.set(connection.destination.destinationId, [
        ...(connectionsByConnectorId.get(connection.destination.destinationId) || []),
        connection,
      ]);
    });
    return {
      connections,
      connectionsByConnectorId,
    };
  };
};

interface ConnectionListTransformed {
  connections: WebBackendConnectionListItem[];
  connectionsByConnectorId: Map<string, WebBackendConnectionListItem[]>;
}

export const useConnectionList = (
  payload: Pick<WebBackendConnectionListRequestBody, "destinationId" | "sourceId"> = {}
) => {
  const workspace = useCurrentWorkspace();
  const REFETCH_CONNECTION_LIST_INTERVAL = 60_000;
  const connectorIds = [
    ...(payload.destinationId ? payload.destinationId : []),
    ...(payload.sourceId ? payload.sourceId : []),
  ];
  const queryKey = getConnectionListQueryKey(connectorIds);
  const queryFn = useConnectionListQuery(workspace.workspaceId, payload.sourceId, payload.destinationId);

  return useQuery(queryKey, queryFn, {
    refetchInterval: REFETCH_CONNECTION_LIST_INTERVAL,
    suspense: true,
  }).data as ConnectionListTransformed;
};

export const useGetConnectionState = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(connectionsKeys.getState(connectionId), () => getState({ connectionId }, requestOptions));
};

export const useGetStateTypeQuery = () => {
  const requestOptions = useRequestOptions();
  return useMutation((connectionId: string) => getStateType({ connectionId }, requestOptions)).mutateAsync;
};

export const useCreateOrUpdateState = () => {
  const requestOptions = useRequestOptions();
  const { formatMessage } = useIntl();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const { trackError } = useAppMonitoringService();
  const { registerNotification } = useNotificationService();

  return useMutation(
    ({ connectionId, connectionState }: ConnectionStateCreateOrUpdate) =>
      createOrUpdateStateSafe({ connectionId, connectionState }, requestOptions),
    {
      onSuccess: (updatedState) => {
        analyticsService.track(Namespace.CONNECTION, Action.CREATE_OR_UPDATE_STATE, {
          actionDescription: "Connection state created or updated",
          connection_id: updatedState.connectionId,
          state_type: updatedState.stateType,
        });
        queryClient.setQueryData(connectionsKeys.getState(updatedState.connectionId), updatedState);
        registerNotification({
          id: `connection.stateUpdateSuccess.${updatedState.connectionId}`,
          text: formatMessage({ id: "connection.state.success" }),
          type: "success",
        });
      },
      onError: (error: Error) => {
        trackError(error);
        registerNotification({
          id: `connection.stateUpdateError.${error.message}`,
          text: error.message,
          type: "error",
        });
      },
    }
  );
};
