import { useQuery } from "react-query";

import { DEFAULT_JSON_MANIFEST_VALUES } from "components/connectorBuilder/types";

import { useConfig } from "config";
import { ConnectorBuilderServerRequestService } from "core/domain/connectorBuilder/ConnectorBuilderServerRequestService";
import {
  ConnectorConfig,
  ConnectorManifest,
  StreamReadRequestBody,
  StreamsListRequestBody,
} from "core/request/ConnectorBuilderClient";
import { DeclarativeComponentSchema } from "core/request/ConnectorManifest";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";

const connectorBuilderKeys = {
  all: ["connectorBuilder"] as const,
  read: (projectId: string, streamName: string) =>
    [...connectorBuilderKeys.all, "read", { projectId, streamName }] as const,
  list: (manifest: ConnectorManifest, config: ConnectorConfig) =>
    [...connectorBuilderKeys.all, "list", { manifest, config }] as const,
  template: ["template"] as const,
  resolve: (manifest?: unknown) => [...connectorBuilderKeys.all, "resolve", { manifest }] as const,
};

function useConnectorBuilderService() {
  const config = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(
    () => new ConnectorBuilderServerRequestService(config.connectorBuilderApiUrl, middlewares),
    [config.connectorBuilderApiUrl, middlewares]
  );
}

export const useReadStream = (projectId: string, params: StreamReadRequestBody) => {
  const service = useConnectorBuilderService();

  return useQuery(connectorBuilderKeys.read(projectId, params.stream), () => service.readStream(params), {
    refetchOnWindowFocus: false,
    enabled: false,
  });
};

export const useListStreams = (params: StreamsListRequestBody) => {
  const service = useConnectorBuilderService();

  return useQuery(connectorBuilderKeys.list(params.manifest, params.config), () => service.listStreams(params), {
    keepPreviousData: true,
    cacheTime: 0,
    retry: false,
  });
};

export const useResolveManifest = () => {
  const service = useConnectorBuilderService();

  return { resolve: (manifest: ConnectorManifest) => service.resolveManifest({ manifest }) };
};

export const useResolvedManifest = (manifest?: unknown) => {
  const service = useConnectorBuilderService();

  return useSuspenseQuery(connectorBuilderKeys.resolve(manifest), async () => {
    if (!manifest) {
      return DEFAULT_JSON_MANIFEST_VALUES;
    }
    try {
      return (await service.resolveManifest({ manifest })).manifest as DeclarativeComponentSchema;
    } catch {
      return undefined;
    }
  });
};
