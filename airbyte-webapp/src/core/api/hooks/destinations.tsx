import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { ConnectionConfiguration } from "area/connector/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";

import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import {
  createDestination,
  deleteDestination,
  getDestination,
  listDestinationsForWorkspace,
  updateDestination,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { DestinationRead, WebBackendConnectionListItem } from "../types/AirbyteClient";
import { useRequestErrorHandler } from "../useRequestErrorHandler";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const destinationsKeys = {
  all: [SCOPE_WORKSPACE, "destinations"] as const,
  lists: () => [...destinationsKeys.all, "list"] as const,
  list: (filters: string) => [...destinationsKeys.lists(), { filters }] as const,
  detail: (destinationId: string) => [...destinationsKeys.all, "details", destinationId] as const,
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration?: ConnectionConfiguration;
}

interface ConnectorProps {
  name: string;
  destinationDefinitionId: string;
}

interface DestinationList {
  destinations: DestinationRead[];
}

const useDestinationList = (): DestinationList => {
  const requestOptions = useRequestOptions();
  const workspace = useCurrentWorkspace();

  return useSuspenseQuery(destinationsKeys.lists(), () =>
    listDestinationsForWorkspace({ workspaceId: workspace.workspaceId }, requestOptions)
  );
};

const useGetDestination = <T extends string | undefined | null>(
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

const useCreateDestination = () => {
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
          connectionConfiguration: values.connectionConfiguration,
        },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(destinationsKeys.lists(), (lst: DestinationList | undefined) => ({
          destinations: [data, ...(lst?.destinations ?? [])],
        }));
      },
      onError,
    }
  );
};

const useDeleteDestination = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const removeConnectionsFromList = useRemoveConnectionsFromList();
  const onError = useRequestErrorHandler("destinations.deleteError");

  return useMutation(
    (payload: { destination: DestinationRead; connectionsWithDestination: WebBackendConnectionListItem[] }) =>
      deleteDestination({ destinationId: payload.destination.destinationId }, requestOptions),
    {
      onSuccess: (_data, ctx) => {
        analyticsService.track(Namespace.DESTINATION, Action.DELETE, {
          actionDescription: "Destination deleted",
          connector_destination: ctx.destination.destinationName,
          connector_destination_definition_id: ctx.destination.destinationDefinitionId,
        });

        queryClient.removeQueries(destinationsKeys.detail(ctx.destination.destinationId));
        queryClient.setQueryData(
          destinationsKeys.lists(),
          (lst: DestinationList | undefined) =>
            ({
              destinations:
                lst?.destinations.filter((conn) => conn.destinationId !== ctx.destination.destinationId) ?? [],
            }) as DestinationList
        );

        const connectionIds = ctx.connectionsWithDestination.map((item) => item.connectionId);
        removeConnectionsFromList(connectionIds);
      },
      onError,
    }
  );
};

const useUpdateDestination = () => {
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

export { useDestinationList, useGetDestination, useCreateDestination, useDeleteDestination, useUpdateDestination };
