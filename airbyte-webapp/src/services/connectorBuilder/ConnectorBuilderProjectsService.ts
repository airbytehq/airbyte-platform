import { QueryClient, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useConfig } from "config";
import { useSuspenseQuery } from "core/api";
import { ConnectorBuilderProjectsRequestService } from "core/domain/connectorBuilder/ConnectorBuilderProjectsRequestService";
import {
  ConnectorBuilderProjectIdWithWorkspaceId,
  DeclarativeManifestVersionRead,
  ConnectorBuilderProjectRead,
  SourceDefinitionIdBody,
} from "core/request/AirbyteClient";
import { DeclarativeComponentSchema } from "core/request/ConnectorManifest";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";

import { useConnectorBuilderService } from "./ConnectorBuilderApiService";
import { SCOPE_WORKSPACE } from "../Scope";

const connectorBuilderProjectsKeys = {
  all: [SCOPE_WORKSPACE, "connectorBuilderProjects"] as const,
  detail: (projectId: string) => [...connectorBuilderProjectsKeys.all, "details", projectId] as const,
  version: (projectId: string, version?: number) =>
    [...connectorBuilderProjectsKeys.all, "version", projectId, version] as const,
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
    (await service.list(workspaceId)).projects.map(
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
    return (
      await service.getConnectorBuilderProjectVersions(workspaceId, project.sourceDefinitionId)
    ).manifestVersions.sort((v1, v2) => v2.version - v1.version);
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

export const useResolvedProjectVersion = (projectId: string, version?: number) => {
  const projectsService = useConnectorBuilderProjectsService();
  const builderService = useConnectorBuilderService();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(
    connectorBuilderProjectsKeys.version(projectId, version),
    async () => {
      if (version === undefined) {
        return null;
      }
      const project = await projectsService.getConnectorBuilderProject(workspaceId, projectId, version);
      if (!project.declarativeManifest?.manifest) {
        return null;
      }
      return (await builderService.resolveManifest({ manifest: project.declarativeManifest?.manifest }))
        .manifest as DeclarativeComponentSchema;
    },
    {
      retry: false,
      cacheTime: Infinity,
      staleTime: Infinity,
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

function updateProjectQueryCache(
  queryClient: QueryClient,
  projectId: string,
  sourceDefinitionId: string,
  version: number
) {
  if (!queryClient.getQueryData(connectorBuilderProjectsKeys.detail(projectId))) {
    return;
  }
  queryClient.setQueryData(
    connectorBuilderProjectsKeys.detail(projectId),
    (project: ConnectorBuilderProjectRead | undefined) => {
      if (!project) {
        throw new Error("invariant: current project not in cache");
      }
      return {
        ...project,
        builderProject: {
          ...project.builderProject,
          sourceDefinitionId,
          activeDeclarativeManifestVersion: version,
        },
      };
    }
  );
}

const FIRST_VERSION = 1;

export const usePublishProject = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionIdBody, Error, BuilderProjectPublishBody>(
    ({ name, projectId, description, manifest }) =>
      service.publishBuilderProject(workspaceId, projectId, name, description, manifest, FIRST_VERSION),
    {
      onSuccess(data, context) {
        queryClient.removeQueries(connectorBuilderProjectsKeys.versions(context.projectId));
        updateProjectQueryCache(queryClient, context.projectId, data.sourceDefinitionId, FIRST_VERSION);
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
        if (context.useAsActiveVersion) {
          updateProjectQueryCache(queryClient, context.projectId, context.sourceDefinitionId, context.version);
        }
      },
    }
  );
};

export interface ChangeVersionContext {
  sourceDefinitionId: string;
  builderProjectId: string;
  version: number;
}

export const useChangeVersion = () => {
  const service = useConnectorBuilderProjectsService();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, ChangeVersionContext>(
    async (context) => {
      return service.changeVersion(workspaceId, context.sourceDefinitionId, context.version);
    },
    {
      onSuccess: (_data, { sourceDefinitionId, version: newVersion, builderProjectId }) => {
        updateProjectQueryCache(queryClient, builderProjectId, sourceDefinitionId, newVersion);
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.versions(builderProjectId),
          (versions: DeclarativeManifestVersionRead[] | undefined) => {
            if (!versions) {
              return [];
            }
            return versions.map((version) =>
              version.version === newVersion ? { ...version, isActive: true } : { ...version, isActive: false }
            );
          }
        );
        queryClient.setQueryData(
          connectorBuilderProjectsKeys.list(workspaceId),
          (list: BuilderProject[] | undefined) => {
            if (!list) {
              return [];
            }
            return list.map((project) => {
              if (project.sourceDefinitionId === sourceDefinitionId) {
                return { ...project, version: newVersion };
              }
              return project;
            });
          }
        );
      },
    }
  );
};
