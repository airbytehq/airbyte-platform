import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { useIntl } from "react-intl";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useInvalidateWorkspaceStateQuery, useSuspenseQuery } from "core/api";
import { SyncSchema } from "core/domain/catalog";
import { WebBackendConnectionService } from "core/domain/connection";
import { ConnectionService } from "core/domain/connection/ConnectionService";
import { getFrequencyFromScheduleData } from "core/services/analytics";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { useInitService } from "services/useInitService";

import { useAppMonitoringService } from "./AppMonitoringService";
import { useNotificationService } from "./Notification";
import { useCurrentWorkspace } from "./useWorkspace";
import { useConfig } from "../../config";
import {
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionStatus,
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
  WebBackendConnectionUpdate,
} from "../../core/request/AirbyteClient";
import { SCOPE_WORKSPACE } from "../../services/Scope";
import { useDefaultRequestMiddlewares } from "../../services/useDefaultRequestMiddlewares";

export const connectionsKeys = {
  all: [SCOPE_WORKSPACE, "connections"] as const,
  lists: (sourceOrDestinationIds: string[] = []) => [...connectionsKeys.all, "list", ...sourceOrDestinationIds],
  detail: (connectionId: string) => [...connectionsKeys.all, "details", connectionId] as const,
  getState: (connectionId: string) => [...connectionsKeys.all, "getState", connectionId] as const,
};

export interface ValuesProps {
  name?: string;
  scheduleData: ConnectionScheduleData | undefined;
  scheduleType: ConnectionScheduleType;
  prefix: string;
  syncCatalog: SyncSchema;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
  operations?: OperationCreate[];
}

interface CreateConnectionProps {
  values: ValuesProps;
  source: SourceRead;
  destination: DestinationRead;
  sourceDefinition?: Pick<SourceDefinitionRead, "sourceDefinitionId">;
  destinationDefinition?: { name: string; destinationDefinitionId: string };
  sourceCatalogId: string | undefined;
}

export const useWebConnectionService = () => {
  const config = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(
    () => new WebBackendConnectionService(config.apiUrl, middlewares),
    [config.apiUrl, middlewares]
  );
};

export function useConnectionService() {
  const config = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(() => new ConnectionService(config.apiUrl, middlewares), [config.apiUrl, middlewares]);
}

export const useSyncConnection = () => {
  const service = useConnectionService();
  const webConnectionService = useWebConnectionService();
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

      return service.sync(connection.connectionId);
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
        await webConnectionService
          .list({ workspaceId })
          .then((updatedConnections) => queryClient.setQueryData(connectionsKeys.lists(), updatedConnections));
      },
    }
  );
};

export const useResetConnection = () => {
  const service = useConnectionService();

  const mutation = useMutation(["useResetConnection"], (connectionId: string) => service.reset(connectionId));
  const activeMutationsCount = useIsMutating(["useResetConnection"]);
  return { ...mutation, isLoading: activeMutationsCount > 0 };
};

export const useResetConnectionStream = (connectionId: string) => {
  const service = useConnectionService();

  return useMutation((streams: ConnectionStream[]) => service.resetStream(connectionId, streams));
};

const useGetConnection = (connectionId: string, options?: { refetchInterval: number }): WebBackendConnectionRead => {
  const service = useWebConnectionService();

  return useSuspenseQuery(connectionsKeys.detail(connectionId), () => service.getConnection(connectionId), options);
};

const useCreateConnection = () => {
  const service = useWebConnectionService();
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
      const response = await service.create({
        sourceId: source.sourceId,
        destinationId: destination.destinationId,
        ...values,
        status: "active",
        sourceCatalogId,
      });

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

const useDeleteConnection = () => {
  const service = useConnectionService();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();

  return useMutation((connection: WebBackendConnectionRead) => service.delete(connection.connectionId), {
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
  });
};

const useUpdateConnection = () => {
  const service = useWebConnectionService();
  const queryClient = useQueryClient();

  return useMutation((connectionUpdate: WebBackendConnectionUpdate) => service.update(connectionUpdate), {
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
  });
};

/**
 * Sets the enable/disable status of a connection. It will use the useConnectionUpdate method
 * to make sure all caches are properly updated, but in addition will trigger the Reenable/Disable
 * analytic event.
 */
export const useEnableConnection = () => {
  const analyticsService = useAnalyticsService();
  const { trackError } = useAppMonitoringService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const { mutateAsync: updateConnection } = useUpdateConnection();

  return useMutation(
    ({ connectionId, enable }: { connectionId: WebBackendConnectionUpdate["connectionId"]; enable: boolean }) =>
      updateConnection({ connectionId, status: enable ? ConnectionStatus.active : ConnectionStatus.inactive }),
    {
      onError: (error: Error) => {
        trackError(error);
        registerNotification({
          id: `tables.updateFailed.${error.message}`,
          text: `${formatMessage({ id: "connection.updateFailed" })}: ${error.message}`,
          type: "error",
        });
      },
      onSuccess: (connection) => {
        const action = connection.status === ConnectionStatus.active ? Action.REENABLE : Action.DISABLE;

        analyticsService.track(Namespace.CONNECTION, action, {
          frequency: getFrequencyFromScheduleData(connection.scheduleData),
          connector_source: connection.source?.sourceName,
          connector_source_definition_id: connection.source?.sourceDefinitionId,
          connector_destination: connection.destination?.destinationName,
          connector_destination_definition_id: connection.destination?.destinationDefinitionId,
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
  const service = useWebConnectionService();

  return async () => {
    const { connections } = await service.list({ workspaceId, sourceId, destinationId });
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

const useConnectionList = (payload: Pick<WebBackendConnectionListRequestBody, "destinationId" | "sourceId"> = {}) => {
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

const useGetConnectionState = (connectionId: string) => {
  const service = useConnectionService();

  return useSuspenseQuery(connectionsKeys.getState(connectionId), () => service.getState(connectionId));
};

export {
  useConnectionList,
  useGetConnection,
  useUpdateConnection,
  useCreateConnection,
  useDeleteConnection,
  useGetConnectionState,
};
