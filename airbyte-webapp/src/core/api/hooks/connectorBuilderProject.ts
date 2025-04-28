import { QueryClient, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import isArray from "lodash/isArray";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { sourceDefinitionKeys } from "core/api";

import { useBuilderResolveManifestQuery } from "./connectorBuilderApi";
import {
  createConnectorBuilderProject,
  createDeclarativeSourceDefinitionManifest,
  deleteConnectorBuilderProject,
  getConnectorBuilderProject,
  listConnectorBuilderProjects,
  listDeclarativeManifests,
  publishConnectorBuilderProject,
  readConnectorBuilderProjectStream,
  updateConnectorBuilderProject,
  updateConnectorBuilderProjectTestingValues,
  updateDeclarativeManifestVersion,
  getDeclarativeManifestBaseImage,
  createForkedConnectorBuilderProject,
  getConnectorBuilderProjectIdForDefinitionId,
  fullResolveManifestBuilderProject,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  ConnectorBuilderProjectIdWithWorkspaceId,
  DeclarativeManifestVersionRead,
  ConnectorBuilderProjectRead,
  SourceDefinitionIdBody,
  ConnectorBuilderProjectFullResolveRequestBody,
  ConnectorBuilderProjectStreamReadRequestBody,
  ConnectorBuilderProjectStreamRead,
  ConnectorBuilderProjectTestingValuesUpdate,
  ConnectorBuilderProjectTestingValues,
  ConnectorBuilderProjectStreamReadSlicesItem,
  ConnectorBuilderProjectStreamReadSlicesItemPagesItem,
  ConnectorBuilderProjectStreamReadSlicesItemStateItem,
  DeclarativeManifestRequestBody,
  DeclarativeManifestBaseImageRead,
  BaseActorDefinitionVersionInfo,
  ContributionInfo,
  ConnectorBuilderProjectDetailsRead,
  SourceDefinitionId,
  BuilderProjectForDefinitionResponse,
} from "../types/AirbyteClient";
import {
  DeclarativeComponentSchema,
  DeclarativeComponentSchemaStreamsItem,
  NoPaginationType,
  StateDelegatingStreamType,
} from "../types/ConnectorManifest";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const connectorBuilderProjectsKeys = {
  all: [SCOPE_WORKSPACE, "connectorBuilderProjects"] as const,
  detail: (projectId: string) => [...connectorBuilderProjectsKeys.all, "details", projectId] as const,
  version: (projectId: string, version?: number) =>
    [...connectorBuilderProjectsKeys.all, "version", projectId, version] as const,
  versions: (projectId?: string) => [...connectorBuilderProjectsKeys.all, "versions", projectId] as const,
  list: (workspaceId: string) => [...connectorBuilderProjectsKeys.all, "list", workspaceId] as const,
  read: (projectId?: string, streamName?: string) =>
    [...connectorBuilderProjectsKeys.all, "read", projectId, streamName] as const,
  getBaseImage: (version: string) => [...connectorBuilderProjectsKeys.all, "getBaseImage", version] as const,
  getProjectForDefinition: (sourceDefinitionId: string | undefined) =>
    [...connectorBuilderProjectsKeys.all, "getProjectByDefinition", sourceDefinitionId] as const,
  fullResolve: (projectId?: string) => ["builder_project_full_resolve", projectId] as const,
};

export interface BuilderProject {
  name: string;
  version: "draft" | number;
  sourceDefinitionId?: string;
  id: string;
  updatedAt: number;
  hasDraft?: boolean;
  baseActorDefinitionVersionInfo?: BaseActorDefinitionVersionInfo;
  contributionInfo?: ContributionInfo;
}

export interface BuilderProjectWithManifest {
  name: string;
  manifest?: DeclarativeComponentSchema;
  yamlManifest?: string;
  componentsFileContent?: string;
  contributionPullRequestUrl?: string;
  contributionActorDefinitionId?: string;
}

export const useListBuilderProjects = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    connectorBuilderProjectsKeys.list(workspaceId),
    async () =>
      (await listConnectorBuilderProjects({ workspaceId }, requestOptions)).projects.map(
        convertProjectDetailsReadToBuilderProject
      ),
    {
      refetchOnMount: "always",
    }
  );
};

export const convertProjectDetailsReadToBuilderProject = (
  projectDetails: ConnectorBuilderProjectDetailsRead
): BuilderProject => ({
  name: projectDetails.name,
  version:
    typeof projectDetails.activeDeclarativeManifestVersion !== "undefined"
      ? projectDetails.activeDeclarativeManifestVersion
      : "draft",
  updatedAt: projectDetails.updatedAt,
  sourceDefinitionId: projectDetails.sourceDefinitionId,
  id: projectDetails.builderProjectId,
  hasDraft: projectDetails.hasDraft,
  baseActorDefinitionVersionInfo: projectDetails.baseActorDefinitionVersionInfo,
  contributionInfo: projectDetails.contributionInfo,
});

export const useListBuilderProjectVersions = (project?: BuilderProject) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(connectorBuilderProjectsKeys.versions(project?.id), async () => {
    if (!project?.sourceDefinitionId) {
      return [];
    }
    return (
      await listDeclarativeManifests({ workspaceId, sourceDefinitionId: project.sourceDefinitionId }, requestOptions)
    ).manifestVersions.sort((v1, v2) => v2.version - v1.version);
  });
};

export type CreateProjectContext =
  | { name: string; assistSessionId?: string; manifest?: DeclarativeComponentSchema }
  | { name: string; assistSessionId?: string; forkProjectId: string; version?: "draft" | number };

export const useCreateBuilderProject = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<ConnectorBuilderProjectIdWithWorkspaceId, Error, CreateProjectContext>(
    async (context) => {
      const name = context.name;
      let manifest;
      if ("forkProjectId" in context) {
        const { declarativeManifest } = await getConnectorBuilderProject(
          {
            workspaceId,
            builderProjectId: context.forkProjectId,
            version: context.version !== "draft" ? context.version : undefined,
          },
          requestOptions
        );
        manifest = declarativeManifest?.manifest as DeclarativeComponentSchema | undefined;
      } else {
        manifest = context.manifest;
      }
      return createConnectorBuilderProject(
        { workspaceId, builderProject: { name, draftManifest: manifest } },
        requestOptions
      );
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
              updatedAt: Date.now(),
            },
          ]
        );
      },
    }
  );
};

export const useCreateSourceDefForkedBuilderProject = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<ConnectorBuilderProjectIdWithWorkspaceId, Error, SourceDefinitionId>(
    async (sourceDefinitionId) => {
      return createForkedConnectorBuilderProject(
        { workspaceId, baseActorDefinitionId: sourceDefinitionId },
        requestOptions
      );
    },
    {
      onSuccess: () => {
        // invalidate cached projects list
        queryClient.invalidateQueries(connectorBuilderProjectsKeys.list(workspaceId));
      },
    }
  );
};

export const useDeleteBuilderProject = () => {
  const queryClient = useQueryClient();
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, BuilderProject>(
    (builderProject) =>
      deleteConnectorBuilderProject({ workspaceId, builderProjectId: builderProject.id }, requestOptions),
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

export const useBuilderProject = (builderProjectId: string) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    connectorBuilderProjectsKeys.detail(builderProjectId),
    () => getConnectorBuilderProject({ workspaceId, builderProjectId }, requestOptions),
    {
      cacheTime: 0,
      retry: false,
    }
  );
};

export const useResolvedBuilderProjectVersion = (projectId: string, version?: number) => {
  const requestOptions = useRequestOptions();
  const resolveManifestQuery = useBuilderResolveManifestQuery();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery(
    connectorBuilderProjectsKeys.version(projectId, version),
    async () => {
      if (version === undefined) {
        return null;
      }
      const project = await getConnectorBuilderProject(
        { workspaceId, builderProjectId: projectId, version },
        requestOptions
      );
      if (!project.declarativeManifest?.manifest) {
        return null;
      }
      const resolvedManifest = (await resolveManifestQuery(project.declarativeManifest.manifest))
        .manifest as DeclarativeComponentSchema;
      return {
        resolvedManifest,
        componentsFileContent: project.builderProject.componentsFileContent,
      };
    },
    {
      retry: false,
      cacheTime: Infinity,
      staleTime: Infinity,
    }
  );
};

export const useUpdateBuilderProject = (projectId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, BuilderProjectWithManifest>(
    ({
      name,
      manifest,
      yamlManifest,
      contributionActorDefinitionId,
      contributionPullRequestUrl,
      componentsFileContent,
    }) =>
      updateConnectorBuilderProject(
        {
          workspaceId,
          builderProjectId: projectId,
          builderProject: {
            name,
            draftManifest: manifest,
            yamlManifest,
            contributionActorDefinitionId,
            contributionPullRequestUrl,
            componentsFileContent,
          },
        },
        requestOptions
      ),
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
  componentsFileContent?: string;
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

export const usePublishBuilderProject = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionIdBody, Error, BuilderProjectPublishBody>(
    ({ name, projectId, description, manifest, componentsFileContent }) =>
      publishConnectorBuilderProject(
        {
          workspaceId,
          builderProjectId: projectId,
          name,
          initialDeclarativeManifest: {
            description,
            manifest,
            version: FIRST_VERSION,
            spec: {
              documentationUrl: manifest.spec?.documentation_url,
              connectionSpecification: manifest.spec?.connection_specification,
              advancedAuth: manifest.spec?.advanced_auth,
            },
          },
          componentsFileContent,
        },
        requestOptions
      ),
    {
      onSuccess(data, context) {
        queryClient.removeQueries(connectorBuilderProjectsKeys.versions(context.projectId));
        updateProjectQueryCache(queryClient, context.projectId, data.sourceDefinitionId, FIRST_VERSION);
        queryClient.invalidateQueries(sourceDefinitionKeys.lists());
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
  componentsFileContent?: string;
}

export const useReleaseNewBuilderProjectVersion = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, NewVersionBody>(
    ({ sourceDefinitionId, description, version, useAsActiveVersion, manifest, componentsFileContent }) =>
      createDeclarativeSourceDefinitionManifest(
        {
          workspaceId,
          sourceDefinitionId,
          declarativeManifest: {
            description,
            version,
            manifest,
            spec: {
              documentationUrl: manifest.spec?.documentation_url,
              connectionSpecification: manifest.spec?.connection_specification,
              advancedAuth: manifest.spec?.advanced_auth,
            },
          },
          setAsActiveManifest: useAsActiveVersion,
          componentsFileContent,
        },
        requestOptions
      ),
    {
      onSuccess(_data, context) {
        queryClient.removeQueries(connectorBuilderProjectsKeys.versions(context.projectId));
        if (context.useAsActiveVersion) {
          updateProjectQueryCache(queryClient, context.projectId, context.sourceDefinitionId, context.version);
        }
        queryClient.invalidateQueries(sourceDefinitionKeys.lists());
      },
    }
  );
};

export interface ChangeVersionContext {
  sourceDefinitionId: string;
  builderProjectId: string;
  version: number;
}

export const useChangeBuilderProjectVersion = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<void, Error, ChangeVersionContext>(
    async (context) => {
      return updateDeclarativeManifestVersion(
        { workspaceId, sourceDefinitionId: context.sourceDefinitionId, version: context.version },
        requestOptions
      );
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

export const useBuilderProjectReadStream = (
  params: ConnectorBuilderProjectStreamReadRequestBody,
  testStream: DeclarativeComponentSchemaStreamsItem | undefined,
  onSuccess: (data: StreamReadTransformedSlices) => void
) => {
  const requestOptions = useRequestOptions();

  return useQuery<StreamReadTransformedSlices>(
    connectorBuilderProjectsKeys.read(params.builderProjectId, params.streamName),
    async () => {
      if (!testStream) {
        // this shouldn't happen, because read stream can only be triggered when a stream is selected
        throw new Error("No test stream provided - this state should not be reached!");
      }

      const streamRead = await readConnectorBuilderProjectStream(params, requestOptions);
      return transformSlices(streamRead, testStream);
    },
    {
      refetchOnWindowFocus: false,
      enabled: false,
      onSuccess,
    }
  );
};

export const useBuilderProjectFullResolveManifest = (params: ConnectorBuilderProjectFullResolveRequestBody) => {
  const requestOptions = useRequestOptions();

  const projectId = params.builderProjectId;

  return useQuery(
    connectorBuilderProjectsKeys.fullResolve(projectId),
    () => fullResolveManifestBuilderProject(params, requestOptions),
    {
      enabled: false,
      refetchOnWindowFocus: false,
    }
  );
};

export const useCancelBuilderProjectStreamRead = (builderProjectId: string, streamName: string) => {
  const queryClient = useQueryClient();
  return () => {
    queryClient.cancelQueries(connectorBuilderProjectsKeys.read(builderProjectId, streamName));
  };
};

export type Page = ConnectorBuilderProjectStreamReadSlicesItemPagesItem & {
  state?: ConnectorBuilderProjectStreamReadSlicesItemStateItem[];
};

export type Slice = Omit<ConnectorBuilderProjectStreamReadSlicesItem, "pages" | "state"> & {
  pages: Page[];
};

export type StreamReadTransformedSlices = Omit<ConnectorBuilderProjectStreamRead, "slices"> & {
  slices: Slice[];
};

const transformSlices = (
  streamReadData: ConnectorBuilderProjectStreamRead,
  stream: DeclarativeComponentSchemaStreamsItem
): StreamReadTransformedSlices => {
  if (stream.type === StateDelegatingStreamType.StateDelegatingStream) {
    return streamReadData;
  }

  // With the addition of ResumableFullRefresh, when pagination is configured and both incremental_sync and
  // partition_routers are NOT configured, the CDK splits up each page into a separate slice, each with its own state.
  // This is to allow full refresh syncs to resume from the last page read in the event of a failure.
  // To keep the Builder UI consistent and show the page selection controls whenever pagination is configured, we check
  // for this scenario and if so, group all of the pages into a single slice, but with their individual states set
  // on each page. This way, the user can still see the unique state of each page in the `State` tab, and will also
  // always see the page selection controls when pagination is configured.
  if (
    stream.retriever.type === "SimpleRetriever" &&
    stream.retriever?.paginator &&
    stream.retriever?.paginator?.type !== NoPaginationType.NoPagination &&
    !stream.incremental_sync &&
    (!stream.retriever?.partition_router ||
      (isArray(stream.retriever?.partition_router) && stream.retriever?.partition_router.length === 0)) &&
    streamReadData.slices.every((slice) => slice.pages.length === 1)
  ) {
    return {
      ...streamReadData,
      slices: [
        {
          pages: streamReadData.slices.map((slice) => ({
            ...slice.pages[0],
            state: slice.state,
          })),
        },
      ],
    };
  }

  // Pages are grouped into slices normally, so just set the slice state on each inner page
  return {
    ...streamReadData,
    slices: streamReadData.slices.map((slice) => {
      return {
        ...slice,
        pages: slice.pages.map((page) => {
          return {
            ...page,
            state: slice.state,
          };
        }),
      };
    }),
  };
};

export const useBuilderProjectUpdateTestingValues = (builderProjectId: string, onSettled?: () => void) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<
    ConnectorBuilderProjectTestingValues,
    Error,
    Omit<ConnectorBuilderProjectTestingValuesUpdate, "builderProjectId" | "workspaceId">
  >(
    ({ spec, testingValues }) =>
      updateConnectorBuilderProjectTestingValues(
        { workspaceId, builderProjectId, spec, testingValues },
        requestOptions
      ),
    {
      onSettled,
    }
  );
};

export const useGetBuilderProjectBaseImage = (params: DeclarativeManifestRequestBody) => {
  const requestOptions = useRequestOptions();

  return useQuery<DeclarativeManifestBaseImageRead>(
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    connectorBuilderProjectsKeys.getBaseImage(params.manifest.version),
    () => getDeclarativeManifestBaseImage(params, requestOptions)
  );
};

export const useGetBuilderProjectIdByDefinitionId = (sourceDefinitionId: string | undefined) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useQuery<BuilderProjectForDefinitionResponse>(
    connectorBuilderProjectsKeys.getProjectForDefinition(sourceDefinitionId),
    () => {
      if (sourceDefinitionId) {
        return getConnectorBuilderProjectIdForDefinitionId(
          { workspaceId, actorDefinitionId: sourceDefinitionId },
          requestOptions
        );
      }
      return {
        builderProjectId: null,
      };
    }
  );
};
