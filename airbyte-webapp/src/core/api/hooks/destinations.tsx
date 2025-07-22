import { InfiniteData, useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { ConnectionConfiguration } from "area/connector/types";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";

import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import {
  createDestination,
  deleteDestination,
  discoverCatalogForDestination,
  getCatalogForConnection,
  getDestination,
  listDestinationsForWorkspace,
  updateDestination,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  ActorListFilters,
  ActorListSortKey,
  DestinationRead,
  DestinationReadList,
  ScopedResourceRequirements,
} from "../types/AirbyteClient";
import { useRequestErrorHandler } from "../useRequestErrorHandler";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const destinationsKeys = {
  all: [SCOPE_WORKSPACE, "destinations"] as const,
  lists: () => [...destinationsKeys.all, "list"] as const,
  list: (filters: ActorListFilters = {}, sortKey?: ActorListSortKey) =>
    [
      ...destinationsKeys.lists(),
      `searchTerm:${filters.searchTerm ?? ""}`,
      `states:${filters.states && filters.states.length > 0 ? filters.states.join(",") : ""}`,
      `sortKey:${sortKey ?? ""}`,
    ] as const,
  detail: (destinationId: string) => [...destinationsKeys.all, "details", destinationId] as const,
  discover: (destinationId: string) => [...destinationsKeys.all, "discover", destinationId] as const,
  catalogByConnectionId: (connectionId: string) =>
    [...destinationsKeys.all, "catalogByConnectionId", connectionId] as const,
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration: ConnectionConfiguration;
  resourceAllocation?: ScopedResourceRequirements;
}

interface ConnectorProps {
  name: string;
  destinationDefinitionId: string;
}

export const useDestinationList = ({
  pageSize = 25,
  filters,
  sortKey,
}: { pageSize?: number; filters?: ActorListFilters; sortKey?: ActorListSortKey } = {}) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useInfiniteQuery({
    queryKey: destinationsKeys.list(filters, sortKey),
    queryFn: async ({ pageParam: cursor }) => {
      return listDestinationsForWorkspace({ workspaceId, pageSize, cursor, filters, sortKey }, requestOptions);
    },
    useErrorBoundary: true,
    getPreviousPageParam: () => undefined, // Cursor based pagination on this endpoint does not support going back
    getNextPageParam: (lastPage) =>
      lastPage.destinations.length < pageSize ? undefined : lastPage.destinations.at(-1)?.destinationId,
  });
};

export const useGetDestination = <T extends string | undefined | null>(
  destinationId: T
): T extends string ? DestinationRead : DestinationRead | undefined => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    destinationsKeys.detail(destinationId ?? ""),
    () => getDestination({ destinationId: destinationId ?? "" }, requestOptions),
    {
      enabled: isDefined(destinationId),
    }
  );
};

export const useInvalidateDestination = <T extends string | undefined | null>(destinationId: T): (() => void) => {
  const queryClient = useQueryClient();

  return useCallback(() => {
    queryClient.invalidateQueries(destinationsKeys.detail(destinationId ?? ""));
  }, [queryClient, destinationId]);
};

export const useCreateDestination = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspace = useCurrentWorkspace();
  const onError = useRequestErrorHandler("destinations.createError");

  return useMutation(
    async (createDestinationPayload: { values: ValuesProps; destinationConnector?: ConnectorProps }) => {
      const { values, destinationConnector } = createDestinationPayload;

      if (!destinationConnector?.destinationDefinitionId) {
        throw new Error("No Destination Definition Provided");
      }

      return createDestination(
        {
          name: values.name,
          destinationDefinitionId: destinationConnector?.destinationDefinitionId,
          workspaceId: workspace.workspaceId,
          connectionConfiguration: values.connectionConfiguration ?? {},
          resourceAllocation: values.resourceAllocation,
        },
        requestOptions
      );
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries(destinationsKeys.lists());
      },
      onError,
    }
  );
};

export const useDeleteDestination = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const removeConnectionsFromList = useRemoveConnectionsFromList();
  const onError = useRequestErrorHandler("destinations.deleteError");

  return useMutation(
    (payload: { destination: DestinationRead }) =>
      deleteDestination({ destinationId: payload.destination.destinationId }, requestOptions),
    {
      onSuccess: (_data, ctx) => {
        analyticsService.track(Namespace.DESTINATION, Action.DELETE, {
          actionDescription: "Destination deleted",
          connector_destination: ctx.destination.destinationName,
          connector_destination_definition_id: ctx.destination.destinationDefinitionId,
        });

        queryClient.removeQueries(destinationsKeys.detail(ctx.destination.destinationId));
        queryClient.setQueriesData(
          destinationsKeys.lists(),
          (oldData: InfiniteData<DestinationReadList> | undefined) => {
            return oldData
              ? {
                  ...oldData,
                  pages: oldData.pages.map((page) => ({
                    ...page,
                    destinations: page.destinations.filter(
                      (destination) => destination.destinationId !== ctx.destination.destinationId
                    ),
                  })),
                }
              : oldData;
          }
        );

        removeConnectionsFromList({ destinationId: ctx.destination.destinationId });
      },
      onError,
    }
  );
};

export const useUpdateDestination = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const onError = useRequestErrorHandler("destinations.updateError");

  return useMutation(
    (updateDestinationPayload: { values: ValuesProps; destinationId: string }) => {
      return updateDestination(
        {
          name: updateDestinationPayload.values.name,
          destinationId: updateDestinationPayload.destinationId,
          connectionConfiguration: updateDestinationPayload.values.connectionConfiguration,
          resourceAllocation: updateDestinationPayload.values.resourceAllocation,
        },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(destinationsKeys.detail(data.destinationId), data);
      },
      onError,
    }
  );
};

export const useDiscoverDestination = (destinationId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    destinationsKeys.discover(destinationId),
    async () => {
      return discoverCatalogForDestination({ destinationId, disableCache: true }, requestOptions);
    },
    {
      useErrorBoundary: true,
      cacheTime: 0, // As soon as the query is not used, it should be removed from the cache
      staleTime: 1000 * 60 * 20, // A discovered schema should be valid for max 20 minutes on the client before refetching
    }
  );
};

// Gets the destination catalog that a connection was configured with
export const useDestinationCatalogByConnectionId = (connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(destinationsKeys.catalogByConnectionId(connectionId), () =>
    getCatalogForConnection({ connectionId }, requestOptions)
  );
};
