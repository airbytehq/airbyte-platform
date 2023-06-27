import { useQuery } from "@tanstack/react-query";

import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { SourceDefinitionSpecificationService } from "core/domain/connector/SourceDefinitionSpecificationService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { isDefined } from "utils/common";

import { SCOPE_WORKSPACE } from "../Scope";

export const sourceDefinitionSpecificationKeys = {
  all: [SCOPE_WORKSPACE, "sourceDefinitionSpecification"] as const,
  detail: (sourceDefId: string | number, sourceId?: string) =>
    [...sourceDefinitionSpecificationKeys.all, "details", { sourceDefId, sourceId }] as const,
};

function useGetService(): SourceDefinitionSpecificationService {
  const { apiUrl } = useConfig();
  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(
    () => new SourceDefinitionSpecificationService(apiUrl, requestAuthMiddleware),
    [apiUrl, requestAuthMiddleware]
  );
}

export const useGetSourceDefinitionSpecification = (sourceDefinitionId: string, sourceId?: string) => {
  const service = useGetService();

  const { workspaceId } = useCurrentWorkspace();
  return useSuspenseQuery(sourceDefinitionSpecificationKeys.detail(sourceDefinitionId, sourceId), () => {
    if (sourceId) {
      return service.getForSource(sourceId);
    }

    return service.get(sourceDefinitionId, workspaceId);
  });
};

export const useGetSourceDefinitionSpecificationAsync = (id: string | null) => {
  const service = useGetService();
  const { workspaceId } = useCurrentWorkspace();

  const escapedId = id ?? "";
  return useQuery(sourceDefinitionSpecificationKeys.detail(escapedId), () => service.get(escapedId, workspaceId), {
    enabled: isDefined(id),
  });
};
