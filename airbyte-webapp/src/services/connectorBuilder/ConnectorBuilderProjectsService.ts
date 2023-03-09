import { useMutation, useQueryClient } from "react-query";

import { useConfig } from "config";
import { ConnectorBuilderProjectsRequestService } from "core/domain/connectorBuilder/ConnectorBuilderProjectsRequestService";
import { ConnectorBuilderProjectIdWithWorkspaceId } from "core/request/AirbyteClient";
import { DeclarativeComponentSchema } from "core/request/ConnectorManifest";
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

export interface BuilderProject {
  name: string;
  version: string;
  id: string;
}

export interface BuilderProjectWithManifest {
  name: string;
  manifest?: DeclarativeComponentSchema;
}

export const useListProjects = () => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(connectorBuilderProjectsKeys.list(workspaceId), async () =>
    (await service.list(workspaceId)).projects.map((projectDetails) => ({
      name: projectDetails.name,
      // TODO: set version based on activeDeclarativeManifestVersion once it is added to the API
      version: "draft",
      id: projectDetails.builderProjectId,
    }))
  );
};

export type CreateProjectContext =
  | { name: string; manifest?: DeclarativeComponentSchema }
  | { name: string; forkProjectId: string };

export const useCreateProject = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<ConnectorBuilderProjectIdWithWorkspaceId, Error, CreateProjectContext>(
    async (context) => {
      const name = context.name;
      let manifest;
      if ("forkProjectId" in context) {
        const { declarativeManifest } = await service.getConnectorBuilderProject(workspaceId, context.forkProjectId);
        manifest = declarativeManifest?.manifest as DeclarativeComponentSchema | undefined;
      } else {
        manifest = context.manifest;
      }
      return service.createBuilderProject(workspaceId, name, manifest);
    },
    {
      onSuccess: ({ builderProjectId }, { name }) => {
        // add created project to cached list
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.list(workspaceId),
          (list: BuilderProject[] | undefined) => [
            ...(list || []),
            {
              id: builderProjectId,
              name,
              version: "draft",
            },
          ]
        );
      },
    }
  );
};

export const useDeleteProject = () => {
  const queryClient = useQueryClient();
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, BuilderProject>(
    (builderProject) => service.deleteBuilderProject(workspaceId, builderProject.id),
    {
      onMutate: (builderProject) => {
        queryClient.removeQueries(connectorBuilderProjectsKeys.detail(builderProject.id));
        // optimistically remove the project from the list
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.list(workspaceId),
          (list: BuilderProject[] | undefined) => list?.filter((project) => project.id !== builderProject.id) ?? []
        );
      },
      onError: (_error, builderProject) => {
        // put the project back on error
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.list(workspaceId),
          (list: BuilderProject[] | undefined) => [...(list || []), builderProject]
        );
      },
    }
  );
};

export const useProject = (projectId: string) => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    connectorBuilderProjectsKeys.detail(projectId),
    () => service.getConnectorBuilderProject(workspaceId, projectId),
    {
      cacheTime: 0,
      retry: false,
    }
  );
};

export const useUpdateProject = (projectId: string) => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, BuilderProjectWithManifest>(
    ({ name, manifest }) => service.updateBuilderProject(workspaceId, projectId, name, manifest),
    {
      onSuccess: (_data, { name }) => {
        // update listing and detail cache on update
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.list(workspaceId),
          (list: BuilderProject[] | undefined) =>
            list?.map((project) => (project.id === projectId ? { ...project, name } : project)) ?? []
        );
      },
    }
  );
};
