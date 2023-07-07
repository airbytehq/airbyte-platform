import { useQuery } from "@tanstack/react-query";

import { DEFAULT_JSON_MANIFEST_VALUES } from "components/connectorBuilder/types";

import { listStreams, readStream, resolveManifest } from "../generated/ConnectorBuilderClient";
import {
  ConnectorConfig,
  ConnectorManifest,
  StreamRead,
  StreamReadRequestBody,
  StreamsListRequestBody,
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

export const useBuilderListStreams = (params: StreamsListRequestBody, enabled = true) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    connectorBuilderKeys.list(params.manifest, params.config),
    () => listStreams(params, requestOptions),
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
  return (manifest: ConnectorManifest) => resolveManifest({ manifest }, requestOptions);
};

export const useBuilderResolvedManifest = (manifest?: ConnectorManifest) => {
  const resolveManifestQuery = useBuilderResolveManifestQuery();

  return useSuspenseQuery(connectorBuilderKeys.resolve(manifest), async () => {
    if (!manifest) {
      return DEFAULT_JSON_MANIFEST_VALUES;
    }
    try {
      return (await resolveManifestQuery(manifest)).manifest as DeclarativeComponentSchema;
    } catch {
      return null;
    }
  });
};
