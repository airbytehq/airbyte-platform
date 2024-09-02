import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { DEFAULT_JSON_MANIFEST_VALUES } from "components/connectorBuilder/types";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { HttpError } from "core/api";
import { useFormatError } from "core/errors";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useDebounceValue } from "core/utils/useDebounceValue";
import { useNotificationService } from "hooks/services/Notification";

import {
  assistV1Process,
  readStream,
  resolveManifest,
  generateContribution,
  checkContribution,
} from "../generated/ConnectorBuilderClient";
import { KnownExceptionInfo } from "../generated/ConnectorBuilderClient.schemas";
import {
  ConnectorConfig,
  ConnectorManifest,
  ResolveManifestRequestBody,
  ResolveManifest,
  StreamRead,
  StreamReadRequestBody,
  AssistV1ProcessRequestBody,
  GenerateContributionRequestBody,
  CheckContributionRead,
  CheckContributionRequestBody,
} from "../types/ConnectorBuilderClient";
import {
  DeclarativeComponentSchema,
  HttpRequesterAuthenticator,
  HttpRequesterHttpMethod,
  PrimaryKey,
  RecordSelector,
  SimpleRetrieverPaginator,
  SpecConnectionSpecification,
} from "../types/ConnectorManifest";
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
  checkContribution: (imageName?: string) => [...connectorBuilderKeys.all, "checkContribution", { imageName }] as const,
  assist: (controller: string, params: AssistV1ProcessRequestBody) =>
    [...connectorBuilderKeys.all, "assist", controller, JSON.stringify(params)] as const,
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

  return useQuery<ResolveManifest, HttpError<KnownExceptionInfo>>(
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

export interface BuilderAssistBaseParams {
  session_id?: string;
  cdk_version?: string;
}
export interface BuilderAssistGlobalParams extends BuilderAssistBaseParams {
  docs_url?: string;
  openapi_spec_url?: string;
  app_name: string;
}
export interface BuilderAssistGlobalUrlParams extends BuilderAssistGlobalParams {
  url_base?: string;
}
export interface BuilderAssistStreamParams extends BuilderAssistGlobalUrlParams {
  stream_name: string;
}
export type BuilderAssistFindUrlBaseParams = BuilderAssistGlobalParams;
export type BuilderAssistFindAuthParams = BuilderAssistGlobalUrlParams;
export type BuilderAssistCreateStreamParams = BuilderAssistStreamParams;
export type BuilderAssistFindStreamPaginatorParams = BuilderAssistStreamParams;
export type BuilderAssistFindStreamMetadataParams = BuilderAssistStreamParams;
export type BuilderAssistFindStreamResponseParams = BuilderAssistStreamParams;

export interface ManifestUpdate {
  cdk_version: string;
  url_base: string | null;
  auth: HttpRequesterAuthenticator | null;
  paginator: SimpleRetrieverPaginator | null;
  connection_specification: SpecConnectionSpecification | null;
  stream_path: string | null;
  stream_http_method: HttpRequesterHttpMethod | null;
  record_selector: RecordSelector | null;
  primary_key: PrimaryKey | null;
}

export interface BuilderAssistBaseResponse {
  metadata: {
    session_id: string;
    server_cdk_version: string;
  };
}
export interface BuilderAssistManifestResponse extends BuilderAssistBaseResponse {
  manifest_update: ManifestUpdate;
}

const useAssistManifestQuery = <T>(controller: string, enabled: boolean, params: AssistV1ProcessRequestBody) => {
  const requestOptions = useRequestOptions();
  const queryKey = connectorBuilderKeys.assist(controller, params);
  const debouncedQueryKey = useDebounceValue(queryKey, 500);

  return useQuery<T, HttpError<KnownExceptionInfo>>(
    debouncedQueryKey,
    // HACK: We need to cast the response from `assistV1Process` to `BuilderAssistManifestResponse`
    // WHY: Because we intentionally did not implement an explicit response type for the assist endpoints
    //      due to the assist service being in an experimental state and the response schema being subject to change frequently
    // TODO: Once the assist service is stable and the response schema is finalized, we should implement explicit response types
    // ISSUE: https://github.com/airbytehq/airbyte-internal-issues/issues/9398
    () => assistV1Process({ controller, ...params }, requestOptions) as Promise<T>,
    {
      enabled,
      keepPreviousData: true,
      cacheTime: Infinity,
      retry: false,
      refetchOnWindowFocus: false,
      refetchOnMount: false,
      refetchOnReconnect: false,
      staleTime: Infinity, // Prevent automatic refetching
    }
  );
};

const hasOneOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.some((key) => !!(params[key] as string)?.trim());
};

const hasAllOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.every((key) => !!(params[key] as string)?.trim());
};

export const useBuilderAssistFindUrlBase = (params: BuilderAssistFindUrlBaseParams) => {
  const hasRequiredParams = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  return useAssistManifestQuery<BuilderAssistManifestResponse>("find_url_base", hasRequiredParams, params);
};

export const useBuilderAssistFindAuth = (params: BuilderAssistFindAuthParams) => {
  const hasRequiredParams = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  return useAssistManifestQuery<BuilderAssistManifestResponse>("find_auth", hasRequiredParams, params);
};

export const useBuilderAssistCreateStream = (params: BuilderAssistCreateStreamParams) => {
  // this one is always enabled, let the server return an error if there is a problem
  return useAssistManifestQuery<BuilderAssistManifestResponse>("create_stream", true, params);
};

export const useBuilderAssistFindStreamPaginator = (params: BuilderAssistFindStreamPaginatorParams) => {
  const hasBase = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  const hasStream = hasAllOf(params, ["app_name", "stream_name"]);
  const hasRequiredParams = hasBase && hasStream;

  return useAssistManifestQuery<BuilderAssistManifestResponse>("find_stream_pagination", hasRequiredParams, params);
};

export const useBuilderAssistStreamMetadata = (params: BuilderAssistFindStreamMetadataParams) => {
  const hasBase = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  const hasStream = hasAllOf(params, ["app_name", "stream_name"]);
  const hasRequiredParams = hasBase && hasStream;

  return useAssistManifestQuery<BuilderAssistManifestResponse>("find_stream_metadata", hasRequiredParams, params);
};

export const useBuilderAssistStreamResponse = (params: BuilderAssistFindStreamResponseParams) => {
  const hasBase = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  const hasStream = hasAllOf(params, ["app_name", "stream_name"]);
  const hasRequiredParams = hasBase && hasStream;

  return useAssistManifestQuery<BuilderAssistManifestResponse>("find_response_structure", hasRequiredParams, params);
};

export interface BuilderAssistFindStreamsParams extends BuilderAssistGlobalUrlParams {
  enabled: boolean;
}
export interface BuilderAssistFoundStream {
  stream_name: string;
}
export interface BuilderAssistFindStreamsResponse extends BuilderAssistBaseResponse {
  streams: BuilderAssistFoundStream[];
}

export const useBuilderAssistFindStreams = (params: BuilderAssistFindStreamsParams) => {
  const isEnabled = params.enabled;
  const hasRequiredParams = hasOneOf(params, ["docs_url", "openapi_spec_url", "app_name"]);
  const enabled = isEnabled && hasRequiredParams;
  return useAssistManifestQuery<BuilderAssistFindStreamsResponse>("find_streams", enabled, params);
};

export const GENERATE_CONTRIBUTION_NOTIFICATION_ID = "generate-contribution-notification";

export const useBuilderGenerateContribution = () => {
  const requestOptions = useRequestOptions();
  const formatError = useFormatError();
  const { registerNotification } = useNotificationService();
  const analyticsService = useAnalyticsService();

  return useMutation((params: GenerateContributionRequestBody) => generateContribution(params, requestOptions), {
    onSuccess: (_date, params) => {
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONTRIBUTE_SUCCESS, {
        actionDescription: "Connector contribution successfully submitted to airbyte repo",
        connector_name: params.name,
        connector_image_name: params.connector_image_name,
      });
    },
    onError: (error: Error, params) => {
      const errorMessage = formatError(error);
      registerNotification({
        id: GENERATE_CONTRIBUTION_NOTIFICATION_ID,
        type: "error",
        text: errorMessage,
      });
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONTRIBUTE_FAILURE, {
        actionDescription: "Connector contribution failed to be submitted to airbyte repo",
        status_code: error instanceof HttpError ? error.status : undefined,
        error_message: errorMessage,
        connector_name: params.name,
        connector_image_name: params.connector_image_name,
      });
    },
  });
};

export const useBuilderCheckContribution = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  const getCachedCheck = useCallback(
    (params: CheckContributionRequestBody) => {
      const queryKey = connectorBuilderKeys.checkContribution(params.connector_image_name);
      return queryClient.getQueryData<CheckContributionRead>(queryKey);
    },
    [queryClient]
  );

  const fetchContributionCheck = useCallback(
    async (params: CheckContributionRequestBody) => {
      try {
        return await queryClient.fetchQuery<CheckContributionRead>(
          connectorBuilderKeys.checkContribution(params.connector_image_name),
          () => checkContribution(params, requestOptions)
        );
      } catch (error) {
        return error as Error;
      }
    },
    [queryClient, requestOptions]
  );

  return {
    getCachedCheck,
    fetchContributionCheck,
  };
};
