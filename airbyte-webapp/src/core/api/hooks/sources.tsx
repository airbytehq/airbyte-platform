import { InfiniteData, useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { v4 as uuidv4 } from "uuid";

import { ConnectionConfiguration } from "area/connector/types";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { isDefined } from "core/utils/common";
import { SourceSetupFlow } from "pages/source/CreateSourcePage/SourceFormWithAgent";

import { useCancelCommand, pollCommandUntilResolved } from "./commands";
import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import { ErrorWithJobInfo } from "../errors";
import {
  createSource,
  deleteSource,
  discoverSchemaForSource,
  getCommandStatus,
  getSource,
  listSourcesForWorkspace,
  runDiscoverCommand,
  updateSource,
  getDiscoverCommandOutput,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  ActorListFilters,
  ActorListSortKey,
  AirbyteCatalog,
  GetDiscoverCommandOutput200,
  ScopedResourceRequirements,
  SourceRead,
  SourceReadList,
} from "../types/AirbyteClient";
import { useRequestErrorHandler } from "../useRequestErrorHandler";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const sourcesKeys = {
  all: [SCOPE_WORKSPACE, "sources"] as const,
  lists: () => [...sourcesKeys.all, "list"] as const,
  list: ({
    pageSize,
    filters = {},
    sortKey,
  }: {
    pageSize?: number;
    filters?: ActorListFilters;
    sortKey?: ActorListSortKey;
  } = {}) =>
    [
      ...sourcesKeys.lists(),
      {
        searchTerm: filters.searchTerm ?? "",
        states: filters.states && filters.states.length > 0 ? filters.states.join(",") : "",
        sortKey: sortKey ?? "",
        pageSize,
      },
    ] as const,
  detail: (sourceId: string) => [...sourcesKeys.all, "details", sourceId] as const,
  discoverSchema: (sourceId: string) => [...sourcesKeys.all, "discoverSchema", sourceId] as const,
  runDiscoverCommand: (commandId: string) => [...sourcesKeys.all, "runDiscover", commandId] as const,
  getCommandStatus: (commandId: string) => [...sourcesKeys.all, "discoverStatus", commandId] as const,
  getDiscoverCommandOutput: (commandId: string) => [...sourcesKeys.all, "discoverOutput", commandId] as const,
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration: ConnectionConfiguration;
  resourceAllocation?: ScopedResourceRequirements;
  setupFlow?: SourceSetupFlow;
}

interface ConnectorProps {
  name: string;
  sourceDefinitionId: string;
}

export const useSourceList = ({
  pageSize = 25,
  filters,
  sortKey,
}: { pageSize?: number; filters?: ActorListFilters; sortKey?: ActorListSortKey } = {}) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useInfiniteQuery({
    queryKey: sourcesKeys.list({ pageSize, filters, sortKey }),
    queryFn: async ({ pageParam: cursor }) => {
      return listSourcesForWorkspace({ workspaceId, pageSize, cursor, filters, sortKey }, requestOptions);
    },
    useErrorBoundary: true,
    getPreviousPageParam: () => undefined, // Cursor based pagination on this endpoint does not support going back
    getNextPageParam: (lastPage) =>
      lastPage.sources.length < pageSize ? undefined : lastPage.sources.at(-1)?.sourceId,
  });
};

export const useGetSource = <T extends string | undefined | null>(
  sourceId: T
): T extends string ? SourceRead : SourceRead | undefined => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    sourcesKeys.detail(sourceId ?? ""),
    () => getSource({ sourceId: sourceId ?? "" }, requestOptions),
    {
      enabled: isDefined(sourceId),
    }
  );
};

export const useInvalidateSource = <T extends string | undefined | null>(sourceId: T): (() => void) => {
  const queryClient = useQueryClient();

  return useCallback(() => {
    queryClient.invalidateQueries(sourcesKeys.detail(sourceId ?? ""));
  }, [queryClient, sourceId]);
};

export const useCreateSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspace = useCurrentWorkspace();
  const analyticsService = useAnalyticsService();
  const onError = useRequestErrorHandler("sources.createError");

  return useMutation(
    async (createSourcePayload: { values: ValuesProps; sourceConnector: ConnectorProps }) => {
      const { values, sourceConnector } = createSourcePayload;
      try {
        // Try to create source
        const result = await createSource(
          {
            name: values.name,
            sourceDefinitionId: sourceConnector?.sourceDefinitionId,
            workspaceId: workspace.workspaceId,
            connectionConfiguration: values.connectionConfiguration,
            resourceAllocation: values.resourceAllocation,
          },
          requestOptions
        );

        return result;
      } catch (e) {
        throw e;
      }
    },
    {
      onSuccess: (_data, ctx) => {
        analyticsService.track(
          Namespace.SOURCE,
          Action.CREATE,
          {
            actionDescription: "Source created",
            connector_source_definition_id: ctx.sourceConnector.sourceDefinitionId,
            connector_source: ctx.sourceConnector.name,
            source_name: ctx.values.name,
            setup_flow: ctx.values.setupFlow ?? "form",
          },
          { sendToPosthog: true }
        );
        queryClient.resetQueries(sourcesKeys.lists());
      },
      onError,
    }
  );
};

export const useDeleteSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const removeConnectionsFromList = useRemoveConnectionsFromList();
  const onError = useRequestErrorHandler("sources.deleteError");

  return useMutation(
    (payload: { source: SourceRead }) => deleteSource({ sourceId: payload.source.sourceId }, requestOptions),
    {
      onSuccess: (_data, ctx) => {
        analyticsService.track(Namespace.SOURCE, Action.DELETE, {
          actionDescription: "Source deleted",
          connector_source: ctx.source.sourceName,
          connector_source_definition_id: ctx.source.sourceDefinitionId,
        });

        queryClient.removeQueries(sourcesKeys.detail(ctx.source.sourceId));
        queryClient.setQueriesData(sourcesKeys.lists(), (oldData: InfiniteData<SourceReadList> | undefined) => {
          return oldData
            ? {
                ...oldData,
                pages: oldData.pages.map((page) => ({
                  ...page,
                  sources: page.sources.filter((source) => source.sourceId !== ctx.source.sourceId),
                })),
              }
            : oldData;
        });

        removeConnectionsFromList({ sourceId: ctx.source.sourceId });
      },
      onError,
    }
  );
};

export const useUpdateSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const onError = useRequestErrorHandler("sources.updateError");

  return useMutation(
    (updateSourcePayload: { values: ValuesProps; sourceId: string }) => {
      return updateSource(
        {
          name: updateSourcePayload.values.name,
          sourceId: updateSourcePayload.sourceId,
          connectionConfiguration: updateSourcePayload.values.connectionConfiguration,
          resourceAllocation: updateSourcePayload.values.resourceAllocation,
        },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourcesKeys.detail(data.sourceId), data);
      },
      onError,
    }
  );
};

const COMMAND_STATUS_POLLING_INTERVAL = 2000;

type SourceDiscoverOutput = GetDiscoverCommandOutput200 & { catalog: AirbyteCatalog; catalogId: string };

export const useDiscoverSourceSchemaMutation = (source: SourceRead) => {
  const requestOptions = useRequestOptions();
  const { mutateAsync: cancelCommand } = useCancelCommand();
  const commandIdRef = useRef<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  // Automatically abort any ongoing request if the component using this hook unmounts
  useEffect(
    () => () => {
      abortControllerRef.current?.abort("Aborting due to component unmount");
      if (commandIdRef.current) {
        cancelCommand(commandIdRef.current);
      }
    },
    [cancelCommand]
  );

  return useMutation<SourceDiscoverOutput>(async (): Promise<SourceDiscoverOutput> => {
    try {
      abortControllerRef.current = new AbortController();
      const commandId = uuidv4();
      commandIdRef.current = commandId;

      await runDiscoverCommand(
        { id: commandId, actor_id: source.sourceId },
        { ...requestOptions, signal: abortControllerRef.current.signal }
      );

      const status = await pollCommandUntilResolved(commandId, requestOptions, abortControllerRef.current.signal);

      if (status === "cancelled") {
        throw new Error(`Discovery command cancelled`);
      }

      const output = await getDiscoverCommandOutput(
        { id: commandId },
        { ...requestOptions, signal: abortControllerRef.current.signal }
      );

      // The open API spec does not guarantee that a source catalog is present in the response
      if (!output.catalog || !output.catalogId) {
        throw new Error("No catalog found in source discovery output");
      }

      return {
        ...output,
        catalog: output.catalog,
        catalogId: output.catalogId,
      };
    } finally {
      commandIdRef.current = null;
      abortControllerRef.current = null;
    }
  });
};

export const useDiscoverSchemaQuery = (source: SourceRead, { useErrorBoundary = true } = {}) => {
  const [commandId, setCommandId] = useState(uuidv4());
  const { mutateAsync: cancelCommand } = useCancelCommand();
  const requestOptions = useRequestOptions();
  const analyticsService = useAnalyticsService();
  const asyncSchemaDiscoveryEnabled = useExperiment("asyncSchemaDiscovery");
  const queryClient = useQueryClient();

  useEffect(
    () => () => {
      if (asyncSchemaDiscoveryEnabled) {
        // cancel command on unmount
        cancelCommand(commandId);
      }
    },
    [commandId, cancelCommand, asyncSchemaDiscoveryEnabled]
  );

  const runCommandQuery = useQuery({
    queryKey: sourcesKeys.runDiscoverCommand(commandId),
    queryFn: async ({ signal }) =>
      runDiscoverCommand({ id: commandId, actor_id: source.sourceId }, { ...requestOptions, signal }),
    useErrorBoundary,
    cacheTime: Infinity,
    staleTime: Infinity,
    enabled: asyncSchemaDiscoveryEnabled,
  });

  const commandStatusQuery = useQuery({
    queryKey: sourcesKeys.getCommandStatus(commandId),
    queryFn: async ({ signal }) => {
      const res = await getCommandStatus({ id: commandId }, { ...requestOptions, signal });
      if (res.status === "cancelled") {
        throw new Error(`Discover schema command was cancelled`);
      }
      return res;
    },
    useErrorBoundary,
    enabled: asyncSchemaDiscoveryEnabled && runCommandQuery.isSuccess && !!runCommandQuery.data?.id, // Only start polling for status once the command has been started
    refetchInterval: (data) =>
      data?.status === "running" || data?.status === "pending" ? COMMAND_STATUS_POLLING_INTERVAL : false, // Poll until the command is completed or fails
    cacheTime: Infinity,
    staleTime: Infinity,
  });

  const commandOutputQuery = useQuery({
    queryKey: sourcesKeys.getDiscoverCommandOutput(commandId),
    queryFn: async ({ signal }) => {
      try {
        const output = await getDiscoverCommandOutput({ id: commandId }, { ...requestOptions, signal });
        if (!output.catalog || !output.catalogId) {
          throw new Error("No catalog found in source discovery output");
        }
        return output;
      } catch (e) {
        const jobInfo = e instanceof ErrorWithJobInfo ? ErrorWithJobInfo.getJobInfo(e) : null;
        analyticsService.track(Namespace.CONNECTION, Action.DISCOVER_SCHEMA, {
          actionDescription: "Discover schema failure",
          connector_source_definition: source.sourceName,
          connector_source_definition_id: source.sourceDefinitionId,
          failure_type: jobInfo?.failureReason?.failureType,
          failure_external_message: jobInfo?.failureReason?.externalMessage,
          failure_internal_message: jobInfo?.failureReason?.internalMessage,
        });
        throw e;
      }
    },
    useErrorBoundary,
    enabled:
      asyncSchemaDiscoveryEnabled && commandStatusQuery.isSuccess && commandStatusQuery.data?.status === "completed",
    cacheTime: Infinity,
    staleTime: Infinity,
  });

  const refetchCommand = useCallback(() => {
    cancelCommand(commandId);
    queryClient.removeQueries({ queryKey: sourcesKeys.runDiscoverCommand(commandId) });
    queryClient.removeQueries({ queryKey: sourcesKeys.getCommandStatus(commandId) });
    queryClient.removeQueries({ queryKey: sourcesKeys.getDiscoverCommandOutput(commandId) });
    setCommandId(uuidv4());
  }, [commandId, queryClient, cancelCommand]);

  const asynchronousQuery = {
    ...commandOutputQuery,
    refetch: refetchCommand,
    error: runCommandQuery.error || commandStatusQuery.error || commandOutputQuery.error,
    // Technically we could use only commandOutputQuery.isLoading here, but in react-query v5 this will only be true
    // when the query itself is fetching (not while it's disabled), so it feels safer to also check the other queries.
    // https://github.com/TanStack/query/issues/3584#issuecomment-1782331608
    isLoading: runCommandQuery.isLoading || commandStatusQuery.isLoading || commandOutputQuery.isLoading,
  };

  const synchronousQuery = useQuery(
    sourcesKeys.discoverSchema(source.sourceId),
    async () => {
      try {
        return discoverSchemaForSource({ sourceId: source.sourceId, disable_cache: true }, requestOptions);
      } catch (e) {
        const jobInfo = e instanceof ErrorWithJobInfo ? ErrorWithJobInfo.getJobInfo(e) : null;
        analyticsService.track(Namespace.CONNECTION, Action.DISCOVER_SCHEMA, {
          actionDescription: "Discover schema failure",
          connector_source_definition: source.sourceName,
          connector_source_definition_id: source.sourceDefinitionId,
          failure_type: jobInfo?.failureReason?.failureType,
          failure_external_message: jobInfo?.failureReason?.externalMessage,
          failure_internal_message: jobInfo?.failureReason?.internalMessage,
        });
        throw e;
      }
    },
    {
      enabled: !asyncSchemaDiscoveryEnabled,
      useErrorBoundary,
      cacheTime: 0, // As soon as the query is not used, it should be removed from the cache
      staleTime: 1000 * 60 * 20, // A discovered schema should be valid for max 20 minutes on the client before refetching
    }
  );

  return asyncSchemaDiscoveryEnabled ? asynchronousQuery : synchronousQuery;
};
