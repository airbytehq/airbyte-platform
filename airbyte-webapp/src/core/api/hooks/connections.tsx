import { Updater, useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getFrequencyFromScheduleData, useAnalyticsService, Action, Namespace } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { RoutePaths } from "pages/routePaths";

import { jobsKeys } from "./jobs";
import { useCurrentWorkspace, useInvalidateWorkspaceStateQuery } from "./workspaces";
import {
  getConnectionStatuses,
  createOrUpdateStateSafe,
  deleteConnection,
  getConnectionDataHistory,
  getConnectionUptimeHistory,
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
import { SCOPE_WORKSPACE } from "../scopes";
import {
  AirbyteCatalog,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionStateCreateOrUpdate,
  ConnectionStatusesRead,
  ConnectionStream,
  DestinationRead,
  JobConfigType,
  JobReadList,
  JobStatus,
  JobWithAttemptsRead,
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
  dataHistory: (connectionId: string) => [...connectionsKeys.all, "dataHistory", connectionId] as const,
  uptimeHistory: (connectionId: string) => [...connectionsKeys.all, "uptimeHistory", connectionId] as const,
  getState: (connectionId: string) => [...connectionsKeys.all, "getState", connectionId] as const,
  statuses: (connectionIds: string[]) => [...connectionsKeys.all, "status", connectionIds],
};

export interface ConnectionValues {
  name?: string;
  scheduleType: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  prefix?: string;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
  syncCatalog: AirbyteCatalog;
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
  const setConnectionRunState = useSetConnectionRunState();

  return useMutation(
    async (connection: WebBackendConnectionRead | WebBackendConnectionListItem) => {
      analyticsService.track(Namespace.CONNECTION, Action.SYNC, {
        actionDescription: "Manual triggered sync",
        connector_source: connection.source?.sourceName,
        connector_source_definition_id: connection.source?.sourceDefinitionId,
        connector_destination: connection.destination?.destinationName,
        connector_destination_definition_id: connection.destination?.destinationDefinitionId,
        frequency: getFrequencyFromScheduleData(connection.scheduleData),
      });

      await syncConnection({ connectionId: connection.connectionId }, requestOptions);
      setConnectionRunState(connection.connectionId, true);
      queryClient.setQueriesData<JobReadList>(
        jobsKeys.useListJobsForConnectionStatus(connection.connectionId),
        (prevJobList) => prependArtificialJobToStatus({ configType: "sync", status: JobStatus.running }, prevJobList)
      );
      queryClient.invalidateQueries(jobsKeys.all(connection.connectionId));
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

/**
 * This function exists because we do not have a proper status API for a connection yet. Instead, we rely on the job list endpoint to determine the current status of a connection.
 * When a sync or reset job is started, we prepend a job to the list to immediately update the conneciton status to running (or cancelled), while re-fetching the actual job list in the background.
 */
export function prependArtificialJobToStatus(
  {
    status,
    configType,
  }: {
    status: JobStatus;
    configType: JobConfigType;
  },
  jobReadList?: JobReadList
): JobReadList {
  const jobs = structuredClone(jobReadList?.jobs ?? []);

  const artificialJob: JobWithAttemptsRead = {
    attempts: [],
    job: {
      id: 999999999,
      status,
      configType: configType ?? "sync",
      createdAt: Math.floor(new Date().getTime() / 1000),
      updatedAt: Math.floor(new Date().getTime() / 1000),
      configId: "fake-config-id",
    },
  };

  return {
    jobs: [artificialJob, ...jobs],
    totalJobCount: jobs.length + 1,
  };
}

export const useResetConnection = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const setConnectionRunState = useSetConnectionRunState();
  const mutation = useMutation(["useResetConnection"], async (connectionId: string) => {
    await resetConnection({ connectionId }, requestOptions);
    setConnectionRunState(connectionId, true);
    queryClient.setQueriesData<JobReadList>(jobsKeys.useListJobsForConnectionStatus(connectionId), (prevJobList) =>
      prependArtificialJobToStatus({ status: JobStatus.running, configType: "reset_connection" }, prevJobList)
    );
  });
  const activeMutationsCount = useIsMutating(["useResetConnection"]);
  return { ...mutation, isLoading: activeMutationsCount > 0 };
};

export const useResetConnectionStream = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const setConnectionRunState = useSetConnectionRunState();
  return useMutation(async (streams: ConnectionStream[]) => {
    await resetConnectionStream({ connectionId, streams }, requestOptions);
    setConnectionRunState(connectionId, true);
    queryClient.setQueriesData<JobReadList>(jobsKeys.useListJobsForConnectionStatus(connectionId), (prevJobList) =>
      prependArtificialJobToStatus({ status: JobStatus.running, configType: "reset_connection" }, prevJobList)
    );
  });
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
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const { registerNotification } = useNotificationService();

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
        // catch error when credits are not enough to enable the connection
        if (error.message.toLowerCase().includes("negative credit balance")) {
          return registerNotification({
            id: "update-connection-credits-problem-error",
            type: "error",
            text: <FormattedMessage id="connection.enable.creditsProblem" />,
            actionBtnText: <FormattedMessage id="connection.enable.creditsProblem.cta" />,
            onAction: () => navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${CloudRoutes.Billing}`),
          });
        }

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

export const useGetConnectionDataHistory = (connectionId: string) => {
  const options = useRequestOptions();

  return useSuspenseQuery(connectionsKeys.dataHistory(connectionId), () =>
    getConnectionDataHistory(
      {
        connectionId,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      },
      options
    )
  );
};

export const useGetConnectionUptimeHistory = (connectionId: string) => {
  const options = useRequestOptions();

  return useSuspenseQuery(connectionsKeys.uptimeHistory(connectionId), () =>
    getConnectionUptimeHistory(
      {
        connectionId,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      },
      options
    )
  );
};

export const useListConnectionsStatuses = (connectionIds: string[]) => {
  const requestOptions = useRequestOptions();
  const queryKey = connectionsKeys.statuses(connectionIds);

  return useSuspenseQuery(queryKey, () => getConnectionStatuses({ connectionIds }, requestOptions), {
    refetchInterval: (data) => {
      // when any of the polled connections is running, refresh 2.5s instead of 10s
      return data?.some(({ isRunning }) => isRunning) ? 2500 : 10000;
    },
  });
};

export const useSetConnectionRunState = () => {
  const queryClient = useQueryClient();

  return (connectionId: string, isRunning: boolean) => {
    queryClient.setQueriesData([SCOPE_WORKSPACE, "connections", "status"], ((data) => {
      if (data) {
        data = data.map((connectionStatus) => {
          if (connectionStatus.connectionId === connectionId) {
            const nextConnectionStatus = structuredClone(connectionStatus); // don't mutate existing object
            nextConnectionStatus.isRunning = isRunning; // set run state
            delete nextConnectionStatus.failureReason; // new runs reset failure state
            return nextConnectionStatus;
          }
          return connectionStatus;
        });
      }
      return data;
    }) as Updater<ConnectionStatusesRead | undefined, ConnectionStatusesRead>);
  };
};
