import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { SourceDefinitionService } from "core/domain/connector/SourceDefinitionService";
import { isDefined } from "core/utils/common";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";

import { connectorDefinitionKeys } from "./ConnectorDefinitions";
import {
  SourceDefinitionCreate,
  SourceDefinitionRead,
  SourceDefinitionReadList,
} from "../../core/request/AirbyteClient";
import { SCOPE_WORKSPACE } from "../Scope";

export const sourceDefinitionKeys = {
  all: [SCOPE_WORKSPACE, "sourceDefinition"] as const,
  lists: () => [...sourceDefinitionKeys.all, "list"] as const,
  listLatest: () => [...sourceDefinitionKeys.all, "listLatest"] as const,
  detail: (id: string) => [...sourceDefinitionKeys.all, "details", id] as const,
};

export function useGetSourceDefinitionService(): SourceDefinitionService {
  const { apiUrl } = useConfig();

  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(
    () => new SourceDefinitionService(apiUrl, requestAuthMiddleware),
    [apiUrl, requestAuthMiddleware]
  );
}

interface SourceDefinitions {
  sourceDefinitions: SourceDefinitionRead[];
  sourceDefinitionMap: Map<string, SourceDefinitionRead>;
}

export const useSourceDefinitionList = (): SourceDefinitions => {
  const service = useGetSourceDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(
    sourceDefinitionKeys.lists(),
    async () => {
      const { sourceDefinitions } = await service.list(workspaceId);
      const sourceDefinitionMap = new Map<string, SourceDefinitionRead>();
      sourceDefinitions.forEach((sourceDefinition) => {
        sourceDefinitionMap.set(sourceDefinition.sourceDefinitionId, sourceDefinition);
      });
      return {
        sourceDefinitions,
        sourceDefinitionMap,
      };
    },
    { suspense: true, staleTime: Infinity }
  ).data as SourceDefinitions;
};

export const useSourceDefinitionMap = (): Map<string, SourceDefinitionRead> => {
  const sourceDefinitions = useSourceDefinitionList();

  return useMemo(() => {
    const sourceDefinitionMap = new Map<string, SourceDefinitionRead>();
    sourceDefinitions.sourceDefinitions.forEach((sourceDefinition) => {
      sourceDefinitionMap.set(sourceDefinition.sourceDefinitionId, sourceDefinition);
    });

    return sourceDefinitionMap;
  }, [sourceDefinitions]);
};

/**
 * This hook calls the list_latest endpoint, which is not needed under most circumstances. Only use this hook if you need the latest source definitions available, for example when prompting the user to update a connector version.
 */
export const useLatestSourceDefinitionList = (): SourceDefinitionReadList => {
  const service = useGetSourceDefinitionService();

  return useSuspenseQuery(sourceDefinitionKeys.listLatest(), () => service.listLatest(), { staleTime: 60_000 });
};

export const useSourceDefinition = <T extends string | undefined>(
  sourceDefinitionId: T
): T extends string ? SourceDefinitionRead : SourceDefinitionRead | undefined => {
  const service = useGetSourceDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    sourceDefinitionKeys.detail(sourceDefinitionId || ""),
    () => service.get({ workspaceId, sourceDefinitionId: sourceDefinitionId || "" }),
    {
      enabled: isDefined(sourceDefinitionId),
    }
  );
};

export const useCreateSourceDefinition = () => {
  const service = useGetSourceDefinitionService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionRead, Error, SourceDefinitionCreate>(
    (sourceDefinition) => service.createCustom({ workspaceId, sourceDefinition }),
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
          const newMap = new Map(oldData?.sourceDefinitionMap);
          newMap.set(data.sourceDefinitionId, data);
          return {
            sourceDefinitions: [data, ...(oldData?.sourceDefinitions ?? [])],
            sourceDefinitionMap: newMap,
          };
        });
      },
    }
  );
};

export const useUpdateSourceDefinition = () => {
  const service = useGetSourceDefinitionService();
  const queryClient = useQueryClient();
  const { trackError } = useAppMonitoringService();

  return useMutation<
    SourceDefinitionRead,
    Error,
    {
      sourceDefinitionId: string;
      dockerImageTag: string;
    }
  >((sourceDefinition) => service.update(sourceDefinition), {
    onSuccess: (data) => {
      queryClient.setQueryData(sourceDefinitionKeys.detail(data.sourceDefinitionId), data);

      queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
        const newMap = new Map(oldData?.sourceDefinitionMap);
        newMap.set(data.sourceDefinitionId, data);
        return {
          sourceDefinitions:
            oldData?.sourceDefinitions.map((sd) => (sd.sourceDefinitionId === data.sourceDefinitionId ? data : sd)) ??
            [],
          sourceDefinitionMap: newMap,
        };
      });

      queryClient.invalidateQueries(connectorDefinitionKeys.count());
    },
    onError: (error: Error) => {
      trackError(error);
    },
  });
};
