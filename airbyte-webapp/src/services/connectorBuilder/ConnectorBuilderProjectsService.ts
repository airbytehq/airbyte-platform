import { useMutation, useQuery, useQueryClient } from "react-query";

import { useConfig } from "config";
import { ConnectorBuilderProjectsRequestService } from "core/domain/connectorBuilder/ConnectorBuilderProjectsRequestService";
import {
  ConnectorBuilderProjectIdWithWorkspaceId,
  ConnectorBuilderProjectRead,
  SourceDefinitionIdBody,
} from "core/request/AirbyteClient";
import { DeclarativeComponentSchema } from "core/request/ConnectorManifest";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";
import { isCloudApp } from "utils/app";

import { SCOPE_WORKSPACE } from "../Scope";

const connectorBuilderProjectsKeys = {
  all: [SCOPE_WORKSPACE, "connectorBuilderProjects"] as const,
  detail: (projectId: string) => [...connectorBuilderProjectsKeys.all, "details", projectId] as const,
  versions: (projectId?: string) => [...connectorBuilderProjectsKeys.all, "versions", projectId] as const,
  list: (workspaceId: string) => [...connectorBuilderProjectsKeys.all, "list", workspaceId] as const,
};

function useConnectorBuilderProjectsService() {
  const { apiUrl } = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(() => new ConnectorBuilderProjectsRequestService(apiUrl, middlewares), [apiUrl, middlewares]);
}

export interface BuilderProject {
  name: string;
  version: "draft" | number;
  sourceDefinitionId?: string;
  id: string;
  hasDraft?: boolean;
}

export interface BuilderProjectWithManifest {
  name: string;
  manifest?: DeclarativeComponentSchema;
}

export const useListProjects = () => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(connectorBuilderProjectsKeys.list(workspaceId), async () =>
    // FIXME this is a temporary solution to avoid calling an API that's not forwarded in cloud environments yet
    isCloudApp()
      ? []
      : (await service.list(workspaceId)).projects.map(
          (projectDetails): BuilderProject => ({
            name: projectDetails.name,
            version:
              typeof projectDetails.activeDeclarativeManifestVersion !== "undefined"
                ? projectDetails.activeDeclarativeManifestVersion
                : "draft",
            sourceDefinitionId: projectDetails.sourceDefinitionId,
            id: projectDetails.builderProjectId,
            hasDraft: projectDetails.hasDraft,
          })
        )
  );
};

export const useListVersions = (project?: BuilderProject) => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(connectorBuilderProjectsKeys.versions(project?.id), async () => {
    if (!project?.sourceDefinitionId) {
      return [];
    }
    return (await service.getConnectorBuilderProjectVersions(workspaceId, project.sourceDefinitionId)).manifestVersions;
  });
};

export const useListVersionsSuspense = (project?: BuilderProject) => {
  const service = useConnectorBuilderProjectsService();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(connectorBuilderProjectsKeys.versions(project?.id), async () => {
    if (!project?.sourceDefinitionId) {
      return [];
    }
    return (await service.getConnectorBuilderProjectVersions(workspaceId, project.sourceDefinitionId)).manifestVersions;
  });
};

export type CreateProjectContext =
  | { name: string; manifest?: DeclarativeComponentSchema }
  | { name: string; forkProjectId: string; version?: "draft" | number };

export const useCreateProject = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<ConnectorBuilderProjectIdWithWorkspaceId, Error, CreateProjectContext>(
    async (context) => {
      const name = context.name;
      let manifest;
      if ("forkProjectId" in context) {
        const { declarativeManifest } = await service.getConnectorBuilderProject(
          workspaceId,
          context.forkProjectId,
          context.version !== "draft" ? context.version : undefined
        );
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
              version: "draft" as const,
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

export interface BuilderProjectPublishBody {
  name: string;
  projectId: string;
  description: string;
  manifest: DeclarativeComponentSchema;
}

export const usePublishProject = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionIdBody, Error, BuilderProjectPublishBody>(
    ({ name, projectId, description, manifest }) =>
      service.publishBuilderProject(workspaceId, projectId, name, description, manifest),
    {
      onSuccess(data, context) {
        queryClient.removeQueries(connectorBuilderProjectsKeys.versions(context.projectId));
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.detail(context.projectId),
          (project: ConnectorBuilderProjectRead | undefined) => {
            if (!project) {
              throw new Error("invariant: current project not in cache");
            }
            return {
              ...project,
              builderProject: {
                ...project.builderProject,
                sourceDefinitionId: data.sourceDefinitionId,
              },
            };
          }
        );
      },
    }
  );
};

export interface NewVersionBody {
  sourceDefinitionId: string;
  projectId: string;
  description: string;
  version: number;
  useAsActiveVersion: boolean;
  manifest: DeclarativeComponentSchema;
}

export const useReleaseNewVersion = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, NewVersionBody>(
    ({ sourceDefinitionId, description, version, useAsActiveVersion, manifest }) =>
      service.releaseNewVersion(workspaceId, sourceDefinitionId, description, version, useAsActiveVersion, manifest),
    {
      onSuccess(_data, context) {
        queryClient.removeQueries(connectorBuilderProjectsKeys.versions(context.projectId));
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.detail(context.projectId),
          (project: ConnectorBuilderProjectRead | undefined) => {
            if (!project) {
              throw new Error("invariant: current project not in cache");
            }
            return {
              ...project,
              builderProject: {
                ...project.builderProject,
                activeDeclarativeManifestVersion: context.useAsActiveVersion
                  ? context.version
                  : project.builderProject.activeDeclarativeManifestVersion,
              },
            };
          }
        );
      },
    }
  );
};
