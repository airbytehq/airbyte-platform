import { useQuery } from "@tanstack/react-query";

import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { DestinationDefinitionSpecificationService } from "core/domain/connector/DestinationDefinitionSpecificationService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { isDefined } from "utils/common";

import { DestinationDefinitionSpecificationRead } from "../../core/request/AirbyteClient";
import { SCOPE_WORKSPACE } from "../Scope";

export const destinationDefinitionSpecificationKeys = {
  all: [SCOPE_WORKSPACE, "destinationDefinitionSpecification"] as const,
  detail: (destinationDefId: string | number, destinationId?: string) =>
    [...destinationDefinitionSpecificationKeys.all, "details", { destinationDefId, destinationId }] as const,
};

function useGetService() {
  const { apiUrl } = useConfig();
  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(
    () => new DestinationDefinitionSpecificationService(apiUrl, requestAuthMiddleware),
    [apiUrl, requestAuthMiddleware]
  );
}

export const useGetDestinationDefinitionSpecification = (
  destinationDefinitionId: string,
  destinationId?: string
): DestinationDefinitionSpecificationRead => {
  const service = useGetService();

  const { workspaceId } = useCurrentWorkspace();
  return useSuspenseQuery(destinationDefinitionSpecificationKeys.detail(destinationDefinitionId, destinationId), () => {
    if (destinationId) {
      return service.getForDestination(destinationId);
    }

    return service.get(destinationDefinitionId, workspaceId);
  });
};

export const useGetDestinationDefinitionSpecificationAsync = (id: string | null) => {
  const service = useGetService();
  const { workspaceId } = useCurrentWorkspace();

  const escapedId = id ?? "";
  return useQuery(destinationDefinitionSpecificationKeys.detail(escapedId), () => service.get(escapedId, workspaceId), {
    enabled: isDefined(id),
  });
};
