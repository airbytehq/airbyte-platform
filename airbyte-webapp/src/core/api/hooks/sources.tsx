import { InfiniteData, useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { ConnectionConfiguration } from "area/connector/types";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";

import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import { ErrorWithJobInfo } from "../errors";
import {
  createSource,
  deleteSource,
  discoverSchemaForSource,
  getSource,
  listSourcesForWorkspace,
  updateSource,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  ActorListFilters,
  ActorListSortKey,
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
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration: ConnectionConfiguration;
  resourceAllocation?: ScopedResourceRequirements;
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
      onSuccess: () => {
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

export const useDiscoverSchemaQuery = (source: SourceRead, { useErrorBoundary = true } = {}) => {
  const requestOptions = useRequestOptions();
  const analyticsService = useAnalyticsService();

  return useQuery(
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
      useErrorBoundary,
      cacheTime: 0, // As soon as the query is not used, it should be removed from the cache
      staleTime: 1000 * 60 * 20, // A discovered schema should be valid for max 20 minutes on the client before refetching
    }
  );
};
