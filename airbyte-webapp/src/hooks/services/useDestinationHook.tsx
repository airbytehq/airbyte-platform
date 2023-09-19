import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { ConnectionConfiguration } from "area/connector/types";
import { useRemoveConnectionsFromList, useSuspenseQuery } from "core/api";
import { DestinationService } from "core/domain/connector/DestinationService";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";
import { useInitService } from "services/useInitService";

import { useRequestErrorHandler } from "./useRequestErrorHandler";
import { useCurrentWorkspace } from "./useWorkspace";
import { useConfig } from "../../config";
import { DestinationRead, WebBackendConnectionListItem } from "../../core/request/AirbyteClient";
import { SCOPE_WORKSPACE } from "../../services/Scope";
import { useDefaultRequestMiddlewares } from "../../services/useDefaultRequestMiddlewares";

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

function useDestinationService() {
  const { apiUrl } = useConfig();
  const requestAuthMiddleware = useDefaultRequestMiddlewares();
  return useInitService(() => new DestinationService(apiUrl, requestAuthMiddleware), [apiUrl, requestAuthMiddleware]);
}

interface DestinationList {
  destinations: DestinationRead[];
}

const useDestinationList = (): DestinationList => {
  const workspace = useCurrentWorkspace();
  const service = useDestinationService();

  return useSuspenseQuery(destinationsKeys.lists(), () => service.list(workspace.workspaceId));
};

const useGetDestination = <T extends string | undefined | null>(
  destinationId: T
): T extends string ? DestinationRead : DestinationRead | undefined => {
  const service = useDestinationService();

  return useSuspenseQuery(destinationsKeys.detail(destinationId ?? ""), () => service.get(destinationId ?? ""), {
    enabled: isDefined(destinationId),
  });
};

export const useInvalidateDestination = <T extends string | undefined | null>(destinationId: T): (() => void) => {
  const queryClient = useQueryClient();

  return useCallback(() => {
    queryClient.invalidateQueries(destinationsKeys.detail(destinationId ?? ""));
  }, [queryClient, destinationId]);
};

const useCreateDestination = () => {
  const service = useDestinationService();
  const queryClient = useQueryClient();
  const workspace = useCurrentWorkspace();
  const onError = useRequestErrorHandler("destinations.createError");

  return useMutation(
    async (createDestinationPayload: { values: ValuesProps; destinationConnector?: ConnectorProps }) => {
      const { values, destinationConnector } = createDestinationPayload;

      if (!destinationConnector?.destinationDefinitionId) {
        throw new Error("No Destination Definition Provided");
      }

      return service.create({
        name: values.name,
        destinationDefinitionId: destinationConnector?.destinationDefinitionId,
        workspaceId: workspace.workspaceId,
        connectionConfiguration: values.connectionConfiguration,
      });
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
  const service = useDestinationService();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const removeConnectionsFromList = useRemoveConnectionsFromList();
  const onError = useRequestErrorHandler("destinations.deleteError");

  return useMutation(
    (payload: { destination: DestinationRead; connectionsWithDestination: WebBackendConnectionListItem[] }) =>
      service.delete(payload.destination.destinationId),
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
            } as DestinationList)
        );

        const connectionIds = ctx.connectionsWithDestination.map((item) => item.connectionId);
        removeConnectionsFromList(connectionIds);
      },
      onError,
    }
  );
};

const useUpdateDestination = () => {
  const service = useDestinationService();
  const queryClient = useQueryClient();
  const onError = useRequestErrorHandler("destinations.updateError");

  return useMutation(
    (updateDestinationPayload: { values: ValuesProps; destinationId: string }) => {
      return service.update({
        name: updateDestinationPayload.values.name,
        destinationId: updateDestinationPayload.destinationId,
        connectionConfiguration: updateDestinationPayload.values.connectionConfiguration,
      });
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
