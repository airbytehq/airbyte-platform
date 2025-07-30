import {
  InfiniteData,
  useInfiniteQuery,
  useIsMutating,
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ExternalLink } from "components/ui/Link";

import { useCurrentConnectionId } from "area/connection/utils/useCurrentConnectionId";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useFormatError } from "core/errors";
import { getFrequencyFromScheduleData, useAnalyticsService, Action, Namespace } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useNotificationService } from "hooks/services/Notification";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

import { useCurrentWorkspace, useInvalidateWorkspaceStateQuery } from "./workspaces";
import { HttpError, HttpProblem } from "../errors";
import {
  getConnectionStatuses,
  getConnectionLastJobPerStream,
  createOrUpdateStateSafe,
  deleteConnection,
  getConnectionDataHistory,
  getConnectionSyncProgress,
  getConnectionUptimeHistory,
  getState,
  getStateType,
  getConnectionEvent,
  clearConnectionStream,
  clearConnection,
  refreshConnectionStream,
  syncConnection,
  listConnectionEvents,
  listConnectionEventsMinimal,
  webBackendCreateConnection,
  webBackendGetConnection,
  webBackendListConnectionsForWorkspace,
  webBackendUpdateConnection,
  webBackendGetConnectionStatusCounts,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  AirbyteCatalog,
  ConnectionEventsListMinimalRequestBody,
  ConnectionEventsRequestBody,
  ConnectionEventType,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionStateCreateOrUpdate,
  ConnectionStatusesRead,
  ConnectionStatusRead,
  ConnectionStream,
  ConnectionSyncStatus,
  DestinationRead,
  JobRead,
  NamespaceDefinitionType,
  OperationCreate,
  RefreshMode,
  SourceDefinitionRead,
  SourceRead,
  WebBackendConnectionListFiltersStatusesItem,
  WebBackendConnectionListItem,
  WebBackendConnectionListRequestBody,
  WebBackendConnectionRead,
  WebBackendConnectionReadList,
  WebBackendConnectionRequestBody,
  WebBackendConnectionUpdate,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const connectionsKeys = {
  all: [SCOPE_WORKSPACE, "connections"] as const,
  lists: (filters: Array<string | string[]> = []) => [...connectionsKeys.all, "list", ...filters],
  detail: (connectionId: string) => [...connectionsKeys.all, "details", connectionId] as const,
  dataHistory: (connectionId: string, jobCount?: number) =>
    [...connectionsKeys.all, "dataHistory", connectionId, ...(jobCount == null ? [] : [jobCount])] as const,
  uptimeHistory: (connectionId: string, jobCount?: number) =>
    [...connectionsKeys.all, "uptimeHistory", connectionId, ...(jobCount == null ? [] : [jobCount])] as const,
  getState: (connectionId: string) => [...connectionsKeys.all, "getState", connectionId] as const,
  statuses: (connectionIds?: string[]) => {
    const key: unknown[] = [...connectionsKeys.all, "status"];
    if (connectionIds) {
      key.push(connectionIds);
    }
    return key;
  },
  syncProgress: (connectionId: string) => [...connectionsKeys.all, "syncProgress", connectionId] as const,
  lastJobPerStream: (connectionId: string) => [...connectionsKeys.all, "lastSyncPerStream", connectionId] as const,
  eventsList: (
    connectionId: string | undefined,
    filters: string | Record<string, string | number | string[] | undefined> = {}
  ) => [...connectionsKeys.all, "eventsList", connectionId, { filters }] as const,
  eventsListMinimal: (requestBody: ConnectionEventsListMinimalRequestBody) =>
    [
      ...connectionsKeys.all,
      "eventsListMinimal",
      requestBody.workspaceId,
      ...requestBody.eventTypes,
      requestBody.createdAtStart,
      requestBody.createdAtEnd,
    ] as const,
  event: (eventId: string) => [...connectionsKeys.all, "event", eventId] as const,
  statusCounts: (workspaceId: string) => [...connectionsKeys.all, "statusCounts", workspaceId] as const,
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

export interface CreateConnectionProps {
  values: ConnectionValues;
  source: SourceRead;
  destination: DestinationRead;
  sourceDefinition?: Pick<SourceDefinitionRead, "sourceDefinitionId">;
  destinationDefinition?: { name: string; destinationDefinitionId: string };
  sourceCatalogId?: string;
  destinationCatalogId?: string;
}

export const useListConnectionEventsInfinite = (
  connectionEventsRequestBody: ConnectionEventsRequestBody,
  enabled: boolean = true,
  pageSize: number = 50
) => {
  const requestOptions = useRequestOptions();
  const queryKey = connectionsKeys.eventsList(connectionEventsRequestBody.connectionId, {
    eventTypes: connectionEventsRequestBody.eventTypes,
    createdAtStart: connectionEventsRequestBody.createdAtStart,
    createdAtEnd: connectionEventsRequestBody.createdAtEnd,
  });

  return useInfiniteQuery(
    queryKey,
    async ({ pageParam = 0 }: { pageParam?: number }) => {
      return {
        data: await listConnectionEvents(
          {
            ...connectionEventsRequestBody,
            pagination: { pageSize, rowOffset: pageSize * pageParam },
          },
          requestOptions
        ),
        pageParam,
      };
    },
    {
      enabled,
      keepPreviousData: true,
      getPreviousPageParam: (firstPage) => (firstPage.pageParam > 0 ? firstPage.pageParam - 1 : undefined),
      getNextPageParam: (lastPage) => (lastPage.data.events.length < pageSize ? undefined : lastPage.pageParam + 1),
    }
  );
};

export const useGetConnectionEvent = (connectionEventId: string | null, connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    connectionsKeys.event(connectionEventId ?? ""),
    async () => {
      return await getConnectionEvent({ connectionEventId: connectionEventId ?? "", connectionId }, requestOptions);
    },
    {
      enabled: !!connectionEventId,
    }
  );
};

export const useGetLastJobPerStream = (connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(connectionsKeys.lastJobPerStream(connectionId), async () => {
    return await getConnectionLastJobPerStream({ connectionId }, requestOptions);
  });
};

export const useGetConnectionSyncProgress = (connectionId: string, enabled: boolean) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    connectionsKeys.syncProgress(connectionId),
    async () => await getConnectionSyncProgress({ connectionId }, requestOptions),
    {
      enabled,
      refetchInterval: 10000,
    }
  );
};

export const useSyncConnection = () => {
  const requestOptions = useRequestOptions();
  const formatError = useFormatError();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const { registerNotification } = useNotificationService();
  const workspaceId = useCurrentWorkspaceId();
  const { formatMessage } = useIntl();
  const setConnectionStatusActiveJob = useSetConnectionStatusActiveJob();

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

      const syncResult = await syncConnection({ connectionId: connection.connectionId }, requestOptions);
      setConnectionStatusActiveJob(connection.connectionId, syncResult.job);
    },
    {
      onError: (error: Error) => {
        trackError(error);
        registerNotification({
          id: `tables.startSyncError.${error.message}`,
          text: `${formatMessage({ id: "connection.startSyncError" })}: ${formatError(error)}`,
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

export const useClearConnection = () => {
  const requestOptions = useRequestOptions();
  const setConnectionStatusActiveJob = useSetConnectionStatusActiveJob();
  const mutation = useMutation(["useClearConnection"], async (connectionId: string) => {
    const clearResult = await clearConnection({ connectionId }, requestOptions);
    setConnectionStatusActiveJob(connectionId, clearResult.job);
  });
  const activeMutationsCount = useIsMutating(["useClearConnection"]);
  return { ...mutation, isLoading: activeMutationsCount > 0 };
};

export const useClearConnectionStream = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  const setConnectionStatusActiveJob = useSetConnectionStatusActiveJob();
  return useMutation(async (streams: ConnectionStream[]) => {
    const clearResult = await clearConnectionStream({ connectionId, streams }, requestOptions);
    setConnectionStatusActiveJob(connectionId, clearResult.job);
  });
};

export const useRefreshConnectionStreams = (connectionId: string) => {
  const queryClient = useQueryClient();
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const setConnectionStatusActiveJob = useSetConnectionStatusActiveJob();

  return useMutation(
    async ({ streams, refreshMode }: { streams?: ConnectionStream[]; refreshMode: RefreshMode }) => {
      return await refreshConnectionStream({ connectionId, streams, refreshMode }, requestOptions);
    },
    {
      onSuccess: (refreshResult) => {
        if (refreshResult.job) {
          setConnectionStatusActiveJob(connectionId, refreshResult.job);
        } else {
          // endpoint returned before the job could be created,
          // invalidate the connection status and hope the job is present in the next fetch
          queryClient.invalidateQueries(connectionsKeys.statuses());
        }
      },
      onError: () => {
        registerNotification({
          id: "connection.actions.error",
          text: formatMessage({ id: "connection.actions.error" }),
        });
      },
    }
  );
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

export const useCurrentConnection = () => {
  const connectionId = useCurrentConnectionId();
  return useGetConnection(connectionId);
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
      destinationCatalogId,
    }: CreateConnectionProps) => {
      const response = await webBackendCreateConnection(
        {
          sourceId: source.sourceId,
          destinationId: destination.destinationId,
          ...values,
          status: "active",
          sourceCatalogId,
          destinationCatalogId,
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
        connection_id: response.connectionId,
      });

      return response;
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(
          connectionsKeys.lists(),
          (connectionList: WebBackendConnectionReadList | undefined) => ({
            ...connectionList,
            // TODO: not sure this is correct, as we would need to place the new connection in the right place in the
            // list. Might be easier to just invalidate the query
            connections: [{ ...(data as WebBackendConnectionListItem) }, ...(connectionList?.connections ?? [])],
          })
        );

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
    async (connection: WebBackendConnectionRead) =>
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
        queryClient.setQueriesData<InfiniteData<WebBackendConnectionReadList>>(connectionsKeys.lists(), (oldData) => {
          if (!oldData) {
            return oldData;
          }
          return {
            ...oldData,
            pages: oldData.pages.map((page) => ({
              ...page,
              connections: page.connections.filter((conn) => conn.connectionId !== connection.connectionId),
            })),
          };
        });
      },
    }
  );
};

export const useUpdateConnectionOptimistically = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const notificationService = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation(async (connectionTagsUpdate: WebBackendConnectionUpdate) => {
    // Using setQueriesData here because we want to update all cached lists regardless of applied filters
    queryClient.setQueriesData<
      InfiniteData<{
        connections: WebBackendConnectionListItem[];
        connectionsByConnectorId: Map<string, WebBackendConnectionListItem[]>;
      }>
    >(connectionsKeys.lists(), (oldData) => {
      if (!oldData) {
        return oldData;
      }

      // Necessary because some properties on WebBackendConnectionUpdate could be null according to the openapi spec,
      // but we don't want to overwrite cached WebBackendConnectionReadListItem properties with null values - we
      // should just ignore them instead
      const nonNullConnectionTagsUpdateProperties = Object.fromEntries(
        Object.entries(connectionTagsUpdate).filter(([_key, value]) => value !== null)
      );

      return {
        pageParams: oldData.pageParams,
        pages: oldData.pages.map((page) => {
          return {
            connectionsByConnectorId: page.connectionsByConnectorId,
            connections: page.connections.map((connection) =>
              connection.connectionId === connectionTagsUpdate.connectionId
                ? { ...connection, ...nonNullConnectionTagsUpdateProperties }
                : connection
            ),
          };
        }),
      };
    });

    queryClient.setQueryData<WebBackendConnectionRead>(
      connectionsKeys.detail(connectionTagsUpdate.connectionId),
      (oldData) => {
        if (!oldData) {
          return oldData;
        }

        // Necessary because some properties on WebBackendConnectionUpdate could be null according to the openapi spec,
        // but we don't want to overwrite cached WebBackendConnectionRead properties with null values - we
        // should just ignore them instead
        const nonNullConnectionTagsUpdateProperties = Object.fromEntries(
          Object.entries(connectionTagsUpdate).filter(([_key, value]) => value !== null)
        );

        return {
          ...oldData,
          ...nonNullConnectionTagsUpdateProperties,
        };
      }
    );

    try {
      const result = await webBackendUpdateConnection(connectionTagsUpdate, requestOptions);
      return result;
    } catch (e) {
      if (!(e instanceof HttpError && HttpProblem.isType(e, "error:connection-conflicting-destination-stream"))) {
        notificationService.registerNotification({
          id: "update-connection-error",
          type: "error",
          text: formatMessage({ id: "connection.updateFailed" }),
        });
      }

      // If the request fails, we need to revert the optimistic update
      queryClient.invalidateQueries<WebBackendConnectionReadList>(connectionsKeys.lists());
      queryClient.invalidateQueries<WebBackendConnectionRead>(
        connectionsKeys.detail(connectionTagsUpdate.connectionId)
      );

      throw e;
    }
  });
};

export const useUpdateConnection = () => {
  const navigate = useNavigate();
  const formatError = useFormatError();
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
        queryClient.setQueryData(
          connectionsKeys.lists(),
          (connectionList: WebBackendConnectionReadList | undefined) => {
            return {
              ...connectionList,
              connections:
                connectionList?.connections.map((connection) => {
                  if (connection.connectionId === updatedConnection.connectionId) {
                    return updatedConnection as WebBackendConnectionListItem;
                  }
                  return connection;
                }) ?? [],
            };
          }
        );
      },
      onError: (error: Error) => {
        // catch error when credits are not enough to enable the connection
        if (error.message.toLowerCase().includes("negative credit balance")) {
          return registerNotification({
            id: "update-connection-credits-problem-error",
            type: "error",
            text: <FormattedMessage id="connection.enable.creditsProblem" />,
            actionBtnText: <FormattedMessage id="connection.enable.creditsProblem.cta" />,
            onAction: () => navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${CloudSettingsRoutePaths.Billing}`),
          });
        }

        if (error instanceof HttpError) {
          if (HttpProblem.isTypeOrSubtype(error, "error:cron-validation") && error.i18nType !== "exact") {
            // Show a specific error message for invalid cron expressions, unless the error already had an exact match for a translation.
            // This is needed since we want to render a link here, which is not possible by just making this a hierarchical translation for
            // error:cron-validation in en.errors.json.
            return registerNotification({
              id: "update-connection-cron-error",
              type: "error",
              text: (
                <FormattedMessage
                  id="form.cronExpression.invalid"
                  values={{
                    lnk: (btnText: React.ReactNode) => (
                      <ExternalLink href={links.cronReferenceLink}>{btnText}</ExternalLink>
                    ),
                  }}
                />
              ),
            });
          }
          if (HttpProblem.isType(error, "error:connection-conflicting-destination-stream")) {
            // We have custom logic for this error that needs access to the form methods, so we should not register the notification here
            return null;
          }

          return registerNotification({
            id: "update-connection-error",
            type: "error",
            text: formatError(error),
          });
        }

        // If there is not user-facing message in the API response, we should fall back to a generic message
        const fallbackKey = error.message && error.message === "common.error" ? "connection.updateFailed" : undefined;
        registerNotification({
          id: "update-connection-error",
          type: "error",
          text: fallbackKey ? <FormattedMessage id={fallbackKey} /> : formatError(error),
        });
      },
    }
  );
};

export const useRemoveConnectionsFromList = () => {
  const queryClient = useQueryClient();

  return useCallback(
    ({
      sourceId,
      destinationId,
    }: { sourceId: string; destinationId?: never } | { sourceId?: never; destinationId: string }) => {
      queryClient.setQueriesData<InfiniteData<WebBackendConnectionReadList>>(connectionsKeys.lists(), (oldData) => {
        if (!oldData) {
          return oldData;
        }
        return {
          ...oldData,
          pages: oldData.pages.map((page) => ({
            ...page,
            connections: page.connections.filter((connection) => {
              if (sourceId) {
                return connection.source.sourceId !== sourceId;
              }
              if (destinationId) {
                return connection.destination.destinationId !== destinationId;
              }
              return true;
            }),
          })),
        };
      });
    },
    [queryClient]
  );
};

interface ConnectionListFilters {
  search: string;
  status: WebBackendConnectionListFiltersStatusesItem | null;
  state: "active" | "inactive" | null;
  sourceDefinitionIds: string[];
  destinationDefinitionIds: string[];
  tagIds: string[];
}

export const useConnectionList = ({
  sourceId,
  destinationId,
  filters,
  sortKey,
  pageSize = 25,
}: Pick<WebBackendConnectionListRequestBody, "destinationId" | "sourceId" | "sortKey" | "pageSize"> & {
  filters?: ConnectionListFilters;
} = {}) => {
  const { workspaceId } = useCurrentWorkspace();
  const requestOptions = useRequestOptions();

  // Create a comprehensive query key that includes all filter parameters
  const queryKey = connectionsKeys.lists([
    ...(sourceId ? [`source-${sourceId.join(",")}`] : []),
    ...(destinationId ? [`destination-${destinationId.join(",")}`] : []),
    ...(filters?.search ? [`search-${filters.search}`] : []),
    ...(filters?.status ? [`status-${filters.status}`] : []),
    ...(filters?.state ? [`state-${filters.state}`] : []),
    ...(filters?.sourceDefinitionIds?.length ? [`sourceDef-${filters.sourceDefinitionIds.join(",")}`] : []),
    ...(filters?.destinationDefinitionIds?.length ? [`destDef-${filters.destinationDefinitionIds.join(",")}`] : []),
    ...(filters?.tagIds?.length ? [`tags-${filters.tagIds.join(",")}`] : []),
    `sort-${sortKey}`,
    `pageSize-${pageSize}`,
  ]);

  return useInfiniteQuery(
    queryKey,
    async ({ pageParam: cursor }) =>
      webBackendListConnectionsForWorkspace(
        {
          workspaceId,
          sourceId,
          destinationId,
          cursor,
          sortKey,
          pageSize,
          filters: filters && {
            searchTerm: filters.search,
            sourceDefinitionIds: filters.sourceDefinitionIds,
            destinationDefinitionIds: filters.destinationDefinitionIds,
            statuses: filters.status ? [filters.status] : undefined,
            states: filters.state ? [filters.state] : undefined,
            tagIds: filters.tagIds,
          },
        },
        requestOptions
      ),
    {
      getNextPageParam: (lastPage) => {
        if (
          (lastPage.page_size !== undefined && lastPage.connections.length < lastPage.page_size) ||
          lastPage.connections.length === 0
        ) {
          return undefined;
        }
        return lastPage.connections.at(-1)?.connectionId;
      },
    }
  );
};

export const useWorkspaceConnectionStatusCounts = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  const REFETCH_CONNECTION_LIST_INTERVAL = 60_000;

  return useQuery(
    connectionsKeys.lists([`workspace-${workspaceId}`]),
    async (): Promise<{ pendingCount: number; successCount: number; failedCount: number }> => {
      const { connections } = await webBackendListConnectionsForWorkspace({ workspaceId }, requestOptions);

      const pendingCount = connections.filter((connection) => connection.isSyncing).length;
      const successCount = connections.filter((connection) => connection.latestSyncJobStatus === "succeeded").length;
      const failedCount = connections.filter(
        (connection) =>
          connection.latestSyncJobStatus === "failed" ||
          connection.latestSyncJobStatus === "cancelled" ||
          connection.latestSyncJobStatus === "incomplete"
      ).length;

      return {
        pendingCount,
        successCount,
        failedCount,
      };
    },
    {
      refetchInterval: REFETCH_CONNECTION_LIST_INTERVAL,
      suspense: true,
    }
  ).data;
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
  const formatError = useFormatError();
  const { formatMessage } = useIntl();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
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
          text: formatError(error),
          type: "error",
        });
      },
    }
  );
};

export const useGetConnectionDataHistory = (connectionId: string, numberOfJobs: number) => {
  const options = useRequestOptions();

  return useQuery(
    connectionsKeys.dataHistory(connectionId, numberOfJobs),
    () => getConnectionDataHistory({ connectionId, numberOfJobs }, options),
    { keepPreviousData: true, staleTime: 30_000 }
  );
};

export const useGetConnectionUptimeHistory = (connectionId: string, numberOfJobs: number) => {
  const options = useRequestOptions();

  return useQuery(
    connectionsKeys.uptimeHistory(connectionId, numberOfJobs),
    () => getConnectionUptimeHistory({ connectionId, numberOfJobs }, options),
    { keepPreviousData: true, staleTime: 30_000 }
  );
};

const CONNECTION_STATUS_REFETCH_INTERVAL = 10_000;

export const useListConnectionsStatuses = (connectionIds: string[]) => {
  const requestOptions = useRequestOptions();
  const queryKey = connectionsKeys.statuses(connectionIds);

  return (
    useSuspenseQuery(queryKey, () => getConnectionStatuses({ connectionIds }, requestOptions), {
      refetchInterval: CONNECTION_STATUS_REFETCH_INTERVAL,
    }) ?? []
  );
};

export const useListConnectionsStatusesAsync = (connectionIds: string[], enabled: boolean = true) => {
  const requestOptions = useRequestOptions();
  const queryKey = connectionsKeys.statuses(connectionIds);

  return (
    useQuery(queryKey, async () => getConnectionStatuses({ connectionIds }, requestOptions), {
      enabled,
      refetchInterval: CONNECTION_STATUS_REFETCH_INTERVAL,
    }) ?? []
  );
};

export const useGetCachedConnectionStatusesById = (connectionIds: string[]) => {
  const queryClient = useQueryClient();
  const queryData = queryClient.getQueriesData<ConnectionStatusesRead>(connectionsKeys.statuses());
  const allStatuses = queryData.flatMap(([_, data]) => data ?? []);

  return connectionIds.reduce<Record<string, ConnectionStatusRead | undefined>>((acc, connectionId) => {
    acc[connectionId] = allStatuses.find((status) => status.connectionId === connectionId);
    return acc;
  }, {});
};

export const useSetConnectionStatusActiveJob = () => {
  const queryClient = useQueryClient();

  return (connectionId: string, activeJob: JobRead) => {
    queryClient.setQueriesData(connectionsKeys.statuses(), (connectionStatuses: ConnectionStatusRead[] | undefined) => {
      return connectionStatuses?.map((connectionStatus) => {
        if (connectionStatus.connectionId === connectionId) {
          return {
            ...connectionStatus,
            activeJob,
            connectionSyncStatus: ConnectionSyncStatus.running,
            failureReason: undefined,
          };
        }
        return connectionStatus;
      });
    });
  };
};

export const CONNECTIONS_GRAPH_EVENT_TYPES = [
  ConnectionEventType.SYNC_SUCCEEDED,
  ConnectionEventType.REFRESH_SUCCEEDED,
  ConnectionEventType.SYNC_INCOMPLETE,
  ConnectionEventType.REFRESH_INCOMPLETE,
  ConnectionEventType.SYNC_FAILED,
  ConnectionEventType.REFRESH_FAILED,
] as const;

export const useGetConnectionsGraphData = (requestBody: ConnectionEventsListMinimalRequestBody) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    connectionsKeys.eventsListMinimal(requestBody),
    async () =>
      listConnectionEventsMinimal({ ...requestBody, eventTypes: [...CONNECTIONS_GRAPH_EVENT_TYPES] }, requestOptions),
    {
      refetchInterval: CONNECTION_STATUS_REFETCH_INTERVAL,
    }
  );
};

export const useGetConnectionStatusesCounts = () => {
  const workspaceId = useCurrentWorkspaceId();
  const requestOptions = useRequestOptions();

  return useQuery(connectionsKeys.statusCounts(workspaceId), () =>
    webBackendGetConnectionStatusCounts({ workspaceId }, requestOptions)
  );
};

export const useGetWorkspacesStatusesCounts = (
  workspaceIds: string[],
  options: { refetchInterval?: boolean; enabled?: boolean } = {}
) => {
  const requestOptions = useRequestOptions();

  return useQueries({
    queries: workspaceIds.map((workspaceId) => ({
      queryKey: connectionsKeys.statusCounts(workspaceId),
      queryFn: async () => ({
        workspaceId,
        statusCounts: await webBackendGetConnectionStatusCounts({ workspaceId }, requestOptions),
      }),
      staleTime: 1000 * 60, // 1 minute
      cacheTime: 1000 * 60 * 2, // 2 minutes
      refetchInterval: options?.refetchInterval ? 1000 * 60 : undefined, // 1 minute if enabled
      enabled: options?.enabled ?? true,
    })),
  });
};
