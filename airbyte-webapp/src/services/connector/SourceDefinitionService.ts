import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { SourceDefinitionService } from "core/domain/connector/SourceDefinitionService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { isDefined } from "utils/common";

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

export const useSourceDefinitionList = (): SourceDefinitionReadList => {
  const service = useGetSourceDefinitionService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(sourceDefinitionKeys.lists(), () => service.list(workspaceId));
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
        queryClient.setQueryData(
          sourceDefinitionKeys.lists(),
          (oldData: { sourceDefinitions: SourceDefinitionRead[] } | undefined) => ({
            sourceDefinitions: [data, ...(oldData?.sourceDefinitions ?? [])],
          })
        );
      },
    }
  );
};

export const useUpdateSourceDefinition = () => {
  const service = useGetSourceDefinitionService();
  const queryClient = useQueryClient();

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

      queryClient.setQueryData(
        sourceDefinitionKeys.lists(),
        (oldData: { sourceDefinitions: SourceDefinitionRead[] } | undefined) => ({
          sourceDefinitions:
            oldData?.sourceDefinitions.map((sd) => (sd.sourceDefinitionId === data.sourceDefinitionId ? data : sd)) ??
            [],
        })
      );

      queryClient.invalidateQueries(connectorDefinitionKeys.count());
    },
  });
};
