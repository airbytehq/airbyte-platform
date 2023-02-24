import { useConfig } from "config";
import { ConnectorBuilderProjectsRequestService } from "core/domain/connectorBuilder/ConnectorBuilderProjectsRequestService";
import { ConnectorManifest } from "core/request/ConnectorManifest";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import { SCOPE_WORKSPACE } from "../Scope";

const connectorBuilderProjectsKeys = {
  all: [SCOPE_WORKSPACE, "connectorBuilderProjects"] as const,
  detail: (projectId: string) => [...connectorBuilderProjectsKeys.all, "details", projectId] as const,
  list: (workspaceId: string) => [...connectorBuilderProjectsKeys.all, "list", workspaceId] as const,
};

function useConnectorBuilderProjectsService() {
  const { apiUrl } = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(() => new ConnectorBuilderProjectsRequestService(apiUrl, middlewares), [apiUrl, middlewares]);
}

export const useListProjects = () => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(connectorBuilderProjectsKeys.list(workspaceId), () => service.list(workspaceId));
};

export const useProject = (projectId: string) => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(connectorBuilderProjectsKeys.detail(workspaceId), () =>
    service.getConnectorBuilderProject(workspaceId, projectId)
  );
};

export const useUpdateProject = (projectId: string) => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return {
    update: (projectName: string, manifest: ConnectorManifest) =>
      service.updateBuilderProject(workspaceId, projectId, projectName, manifest),
  };
};
