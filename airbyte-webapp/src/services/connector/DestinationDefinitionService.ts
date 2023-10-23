import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useConfig } from "config";
import { useSuspenseQuery, connectorDefinitionKeys } from "core/api";
// eslint-disable-next-line import/no-restricted-paths
import { useRequestOptions } from "core/api/useRequestOptions";
import { DestinationDefinitionService } from "core/domain/connector/DestinationDefinitionService";
import { isDefined } from "core/utils/common";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useInitService } from "services/useInitService";

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

  const requestOptions = useRequestOptions();

  return useInitService(() => new DestinationDefinitionService(requestOptions), [apiUrl, requestOptions]);
}

/**
 * This hook calls the list_latest endpoint, which is not needed under most circumstances. Only use this hook if you need the latest destination definitions available, for example when prompting the user to update a connector version.
 */
export const useLatestDestinationDefinitionList = (): DestinationDefinitionReadList => {
  const service = useGetDestinationDefinitionService();

  return useSuspenseQuery(destinationDefinitionKeys.listLatest(), () => service.listLatest(), { staleTime: 60_000 });
};

interface DestinationDefinitions {
  destinationDefinitions: DestinationDefinitionRead[];
  destinationDefinitionMap: Map<string, DestinationDefinitionRead>;
}

export const useDestinationDefinitionList = (): DestinationDefinitions => {
  const service = useGetDestinationDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(
    destinationDefinitionKeys.lists(),
    async () => {
      const { destinationDefinitions } = await service.list(workspaceId);
      const destinationDefinitionMap = new Map<string, DestinationDefinitionRead>();
      destinationDefinitions.forEach((destinationDefinition) => {
        destinationDefinitionMap.set(destinationDefinition.destinationDefinitionId, destinationDefinition);
      });
      return {
        destinationDefinitions,
        destinationDefinitionMap,
      };
    },
    { suspense: true }
  ).data as DestinationDefinitions;
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
        queryClient.setQueryData(destinationDefinitionKeys.lists(), (oldData: DestinationDefinitions | undefined) => {
          const newMap = new Map(oldData?.destinationDefinitionMap);
          newMap.set(data.destinationDefinitionId, data);
          return {
            destinationDefinitions: [data, ...(oldData?.destinationDefinitions ?? [])],
            destinationDefinitionMap: newMap,
          };
        });
      },
    }
  );
};

export const useUpdateDestinationDefinition = () => {
  const service = useGetDestinationDefinitionService();
  const queryClient = useQueryClient();
  const { trackError } = useAppMonitoringService();

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

      queryClient.setQueryData(destinationDefinitionKeys.lists(), (oldData: DestinationDefinitions | undefined) => {
        const newMap = new Map(oldData?.destinationDefinitionMap);
        newMap.set(data.destinationDefinitionId, data);
        return {
          destinationDefinitions:
            oldData?.destinationDefinitions.map((dd) =>
              dd.destinationDefinitionId === data.destinationDefinitionId ? data : dd
            ) ?? [],
          destinationDefinitionMap: newMap,
        };
      });

      queryClient.invalidateQueries(connectorDefinitionKeys.count());
    },
    onError: (error: Error) => {
      trackError(error);
    },
  });
};
