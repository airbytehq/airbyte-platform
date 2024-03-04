import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { isDefined } from "core/utils/common";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";

import { connectorDefinitionKeys } from "./connectorUpdates";
import {
  createCustomSourceDefinition,
  getSourceDefinitionForWorkspace,
  listLatestSourceDefinitions,
  listSourceDefinitionsForWorkspace,
  updateSourceDefinition,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { SourceDefinitionCreate, SourceDefinitionRead, SourceDefinitionReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const sourceDefinitionKeys = {
  all: [SCOPE_WORKSPACE, "sourceDefinition"] as const,
  lists: () => [...sourceDefinitionKeys.all, "list"] as const,
  listLatest: () => [...sourceDefinitionKeys.all, "listLatest"] as const,
  detail: (id: string) => [...sourceDefinitionKeys.all, "details", id] as const,
};

interface SourceDefinitions {
  sourceDefinitions: SourceDefinitionRead[];
  sourceDefinitionMap: Map<string, SourceDefinitionRead>;
}

export const useSourceDefinitionList = (): SourceDefinitions => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(
    sourceDefinitionKeys.lists(),
    async () => {
      const { sourceDefinitions } = await listSourceDefinitionsForWorkspace({ workspaceId }, requestOptions).then(
        ({ sourceDefinitions }) => ({
          sourceDefinitions: sourceDefinitions.sort((a, b) => a.name.localeCompare(b.name)),
        })
      );
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
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(sourceDefinitionKeys.listLatest(), () => listLatestSourceDefinitions(requestOptions), {
    staleTime: 60_000,
  });
};

export const useSourceDefinition = <T extends string | undefined>(
  sourceDefinitionId: T
): T extends string ? SourceDefinitionRead : SourceDefinitionRead | undefined => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    sourceDefinitionKeys.detail(sourceDefinitionId || ""),
    () =>
      getSourceDefinitionForWorkspace({ workspaceId, sourceDefinitionId: sourceDefinitionId || "" }, requestOptions),
    {
      enabled: isDefined(sourceDefinitionId),
    }
  );
};

export const useCreateSourceDefinition = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionRead, Error, SourceDefinitionCreate>(
    (sourceDefinition) => createCustomSourceDefinition({ workspaceId, sourceDefinition }, requestOptions),
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
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { trackError } = useAppMonitoringService();

  return useMutation<
    SourceDefinitionRead,
    Error,
    {
      sourceDefinitionId: string;
      dockerImageTag: string;
    }
  >((sourceDefinition) => updateSourceDefinition(sourceDefinition, requestOptions), {
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
