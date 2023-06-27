import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { DestinationDefinitionService } from "core/domain/connector/DestinationDefinitionService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";
import { isDefined } from "utils/common";

import { connectorDefinitionKeys } from "./ConnectorDefinitions";
import {
  DestinationDefinitionCreate,
  DestinationDefinitionRead,
  DestinationDefinitionReadList,
} from "../../core/request/AirbyteClient";
import { SCOPE_WORKSPACE } from "../Scope";

export const destinationDefinitionKeys = {
  all: [SCOPE_WORKSPACE, "destinationDefinition"] as const,
  lists: () => [...destinationDefinitionKeys.all, "list"] as const,
  listLatest: () => [...destinationDefinitionKeys.all, "listLatest"] as const,
  detail: (id: string) => [...destinationDefinitionKeys.all, "details", id] as const,
};

export function useGetDestinationDefinitionService(): DestinationDefinitionService {
  const { apiUrl } = useConfig();

  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(
    () => new DestinationDefinitionService(apiUrl, requestAuthMiddleware),
    [apiUrl, requestAuthMiddleware]
  );
}

/**
 * This hook calls the list_latest endpoint, which is not needed under most circumstances. Only use this hook if you need the latest destination definitions available, for example when prompting the user to update a connector version.
 */
export const useLatestDestinationDefinitionList = (): DestinationDefinitionReadList => {
  const service = useGetDestinationDefinitionService();

  return useSuspenseQuery(destinationDefinitionKeys.listLatest(), () => service.listLatest(), { staleTime: 60_000 });
};

export const useDestinationDefinitionList = (): DestinationDefinitionReadList => {
  const service = useGetDestinationDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(destinationDefinitionKeys.lists(), () => service.list(workspaceId));
};

export const useDestinationDefinition = <T extends string | undefined>(
  destinationDefinitionId: T
): T extends string ? DestinationDefinitionRead : DestinationDefinitionRead | undefined => {
  const service = useGetDestinationDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    destinationDefinitionKeys.detail(destinationDefinitionId || ""),
    () => service.get({ workspaceId, destinationDefinitionId: destinationDefinitionId || "" }),
    {
      enabled: isDefined(destinationDefinitionId),
    }
  );
};

export const useCreateDestinationDefinition = () => {
  const service = useGetDestinationDefinitionService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<DestinationDefinitionRead, Error, DestinationDefinitionCreate>(
    (destinationDefinition) => service.createCustom({ workspaceId, destinationDefinition }),
    {
      onSuccess: (data) => {
        queryClient.setQueryData(
          destinationDefinitionKeys.lists(),
          (oldData: { destinationDefinitions: DestinationDefinitionRead[] } | undefined) => ({
            destinationDefinitions: [data, ...(oldData?.destinationDefinitions ?? [])],
          })
        );
      },
    }
  );
};

export const useUpdateDestinationDefinition = () => {
  const service = useGetDestinationDefinitionService();
  const queryClient = useQueryClient();

  return useMutation<
    DestinationDefinitionRead,
    Error,
    {
      destinationDefinitionId: string;
      dockerImageTag: string;
    }
  >((destinationDefinition) => service.update(destinationDefinition), {
    onSuccess: (data) => {
      queryClient.setQueryData(destinationDefinitionKeys.detail(data.destinationDefinitionId), data);

      queryClient.setQueryData(
        destinationDefinitionKeys.lists(),
        (oldData: { destinationDefinitions: DestinationDefinitionRead[] } | undefined) => ({
          destinationDefinitions:
            oldData?.destinationDefinitions.map((sd) =>
              sd.destinationDefinitionId === data.destinationDefinitionId ? data : sd
            ) ?? [],
        })
      );

      queryClient.invalidateQueries(connectorDefinitionKeys.count());
    },
  });
};
