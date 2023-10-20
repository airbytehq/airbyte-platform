import { useQuery } from "@tanstack/react-query";

import { DEFAULT_JSON_MANIFEST_VALUES } from "components/connectorBuilder/types";

import { useCurrentWorkspaceId } from "area/workspace/utils";

import { readStream, resolveManifest } from "../generated/ConnectorBuilderClient";
import { KnownExceptionInfo } from "../generated/ConnectorBuilderClient.schemas";
import {
  ConnectorConfig,
  ConnectorManifest,
  ResolveManifestRequestBody,
  ResolveManifest,
  StreamRead,
  StreamReadRequestBody,
} from "../types/ConnectorBuilderClient";
import { DeclarativeComponentSchema } from "../types/ConnectorManifest";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const connectorBuilderKeys = {
  all: ["connectorBuilder"] as const,
  read: (projectId: string, streamName: string) =>
    [...connectorBuilderKeys.all, "read", { projectId, streamName }] as const,
  list: (manifest: ConnectorManifest, config: ConnectorConfig) =>
    [...connectorBuilderKeys.all, "list", { manifest, config }] as const,
  template: ["template"] as const,
  resolve: (manifest?: ConnectorManifest) => [...connectorBuilderKeys.all, "resolve", { manifest }] as const,
  resolveSuspense: (manifest?: ConnectorManifest) =>
    [...connectorBuilderKeys.all, "resolveSuspense", { manifest }] as const,
};

export const useBuilderReadStream = (
  projectId: string,
  params: StreamReadRequestBody,
  onSuccess: (data: StreamRead) => void
) => {
  const requestOptions = useRequestOptions();

  return useQuery(connectorBuilderKeys.read(projectId, params.stream), () => readStream(params, requestOptions), {
    refetchOnWindowFocus: false,
    enabled: false,
    onSuccess,
  });
};

export const useBuilderResolvedManifest = (params: ResolveManifestRequestBody, enabled = true) => {
  const requestOptions = useRequestOptions();

  return useQuery<ResolveManifest, KnownExceptionInfo>(
    connectorBuilderKeys.resolve(params.manifest),
    () => resolveManifest(params, requestOptions),
    {
      keepPreviousData: true,
      cacheTime: 0,
      retry: false,
      enabled,
    }
  );
};

export const useBuilderResolveManifestQuery = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  return (manifest: ConnectorManifest, projectId?: string) =>
    resolveManifest({ manifest, workspace_id: workspaceId, project_id: projectId }, requestOptions);
};

export const useBuilderResolvedManifestSuspense = (manifest?: ConnectorManifest, projectId?: string) => {
  const resolveManifestQuery = useBuilderResolveManifestQuery();

  return useSuspenseQuery(connectorBuilderKeys.resolveSuspense(manifest), async () => {
    if (!manifest) {
      return DEFAULT_JSON_MANIFEST_VALUES;
    }
    try {
      return (await resolveManifestQuery(manifest, projectId)).manifest as DeclarativeComponentSchema;
    } catch {
      return null;
    }
  });
};
