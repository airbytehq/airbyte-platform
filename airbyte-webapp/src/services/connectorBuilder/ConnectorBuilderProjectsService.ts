import { useConfig } from "config";
import { ConnectorBuilderProjectsRequestService } from "core/domain/connectorBuilder/ConnectorBuilderProjectsRequestService";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";

import { SCOPE_WORKSPACE } from "../Scope";

const connectorBuilderProjectsKeys = {
  all: [SCOPE_WORKSPACE, "connectorBuilderProjects"] as const,
  list: (workspaceId: string) => [...connectorBuilderProjectsKeys.all, "list", workspaceId] as const,
};

function useConnectorBuilderProjectsService() {
  const { apiUrl } = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(() => new ConnectorBuilderProjectsRequestService(apiUrl, middlewares), [apiUrl, middlewares]);
}

export const useListProjects = (workspaceId: string) => {
  const service = useConnectorBuilderProjectsService();

  return useSuspenseQuery(connectorBuilderProjectsKeys.list(workspaceId), () => service.list(workspaceId));
};
