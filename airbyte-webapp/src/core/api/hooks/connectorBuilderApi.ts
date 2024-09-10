import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import merge from "lodash/merge";
import omit from "lodash/omit";
import { useCallback } from "react";

import { CDK_VERSION } from "components/connectorBuilder/cdk";
import { DEFAULT_JSON_MANIFEST_VALUES, useBuilderWatch } from "components/connectorBuilder/types";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { HttpError } from "core/api";
import { useFormatError } from "core/errors";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useDebounceValue } from "core/utils/useDebounceValue";
import { useNotificationService } from "hooks/services/Notification";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { ApiCallOptions } from "../apiCall";
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

const OMIT_ASSIST_KEYS_IN_CACHE = ["manifest"]; // just extra metadata that we don't need to factor into cache

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
  assist: (
    controller: string,
    params: AssistV1ProcessRequestBody,
    ignoreCacheKeys: Array<keyof BuilderAssistApiAllParams>
  ) =>
    [
      ...connectorBuilderKeys.all,
      "assist",
      controller,
      JSON.stringify(omit(params, ...OMIT_ASSIST_KEYS_IN_CACHE.concat(ignoreCacheKeys))),
    ] as const,
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

export interface BuilderAssistCoreParams {
  docs_url?: string;
  openapi_spec_url?: string;
  app_name: string;
}

export interface BuilderAssistGlobalMetadataParams {
  cdk_version: string;
  workspace_id: string;
}
export interface BuilderAssistProjectMetadataParams {
  project_id: string;
  manifest: DeclarativeComponentSchema;
  url_base?: string;
  session_id: string;
}
export interface BuilderAssistInputStreamParams {
  stream_name: string;
}
type BuilderAssistUseProject = BuilderAssistCoreParams & BuilderAssistProjectMetadataParams;
type BuilderAssistUseProjectStream = BuilderAssistUseProject & BuilderAssistInputStreamParams;

export type BuilderAssistApiAllParams = BuilderAssistCoreParams &
  BuilderAssistProjectMetadataParams &
  BuilderAssistProjectMetadataParams &
  BuilderAssistInputStreamParams;
export type BuilderAssistInputAllParams = BuilderAssistInputStreamParams;

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

const explicitlyCastedAssistV1Process = <T>(
  controller: string,
  params: AssistV1ProcessRequestBody,
  requestOptions: ApiCallOptions
) => {
  // HACK: We need to cast the response from `assistV1Process` to `BuilderAssistManifestResponse`
  // WHY: Because we intentionally did not implement an explicit response type for the assist endpoints
  //      due to the assist service being in an experimental state and the response schema being subject to change frequently
  // TODO: Once the assist service is stable and the response schema is finalized, we should implement explicit response types
  // ISSUE: https://github.com/airbytehq/airbyte-internal-issues/issues/9398
  return assistV1Process({ controller, ...params }, requestOptions) as Promise<T>;
};

const useAssistProxyQuery = <T>(
  controller: string,
  enabled: boolean,
  input: AssistV1ProcessRequestBody,
  ignoreCacheKeys: Array<keyof BuilderAssistApiAllParams>
) => {
  const requestOptions = useRequestOptions();
  const { params: globalParams } = useAssistGlobalContext();
  const params = merge({}, globalParams, input);
  requestOptions.signal = AbortSignal.timeout(5 * 60 * 1000); // 5 minutes

  const queryKey = connectorBuilderKeys.assist(controller, params, ignoreCacheKeys);
  const debouncedQueryKey = useDebounceValue(queryKey, 500);

  return useQuery<T, HttpError<KnownExceptionInfo>>(
    debouncedQueryKey,
    () => explicitlyCastedAssistV1Process<T>(controller, params, requestOptions),
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

const hasValueWithTrim = (value: unknown) => {
  // if the value is a string, trim first
  if (typeof value === "string") {
    return value.trim() !== "";
  }
  // else check if truthy
  return !!value;
};

const hasOneOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.some((key) => hasValueWithTrim(params[key]));
};

const hasAllOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.every((key) => !!(params[key] as string)?.trim());
};

const useAssistGlobalContext = (): { params: BuilderAssistGlobalMetadataParams } => {
  const params: BuilderAssistGlobalMetadataParams = {
    cdk_version: CDK_VERSION,
    workspace_id: useCurrentWorkspaceId(),
  };
  return { params };
};

const useAssistProjectContext = (): { params: BuilderAssistUseProject; hasRequiredParams: boolean } => {
  const { assistEnabled, projectId, assistSessionId, jsonManifest } = useConnectorBuilderFormState();
  // session id on form
  const params: BuilderAssistUseProject = {
    docs_url: useBuilderWatch("formValues.assist.docsUrl"),
    openapi_spec_url: useBuilderWatch("formValues.assist.openApiSpecUrl"),
    app_name: useBuilderWatch("name") || "Connector",
    url_base: useBuilderWatch("formValues.global.urlBase"),
    project_id: projectId,
    manifest: jsonManifest,
    session_id: assistSessionId,
  };
  const hasBase = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  const hasRequiredParams = assistEnabled && hasBase;
  return { params, hasRequiredParams };
};
const useAssistProjectStreamContext = (
  input: BuilderAssistInputStreamParams
): { params: BuilderAssistUseProjectStream; hasRequiredParams: boolean } => {
  const { params: baseParams, hasRequiredParams: baseRequiredParams } = useAssistProjectContext();
  const params = { ...baseParams, ...input };
  const hasStream = hasAllOf(params, ["stream_name"]);
  const hasRequiredParams = baseRequiredParams && hasStream;
  return { params, hasRequiredParams };
};

export const useBuilderAssistFindUrlBase = () => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_url_base", hasRequiredParams, params, ["url_base"]);
};

export const useBuilderAssistFindAuth = () => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_auth", hasRequiredParams, params, ["url_base"]);
};

export const useBuilderAssistCreateStream = (input: BuilderAssistInputStreamParams) => {
  // this one is always enabled, let the server return an error if there is a problem
  const { params } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("create_stream", true, params, []);
};

export const useBuilderAssistFindStreamPaginator = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_stream_pagination", hasRequiredParams, params, [
    "url_base",
  ]);
};

export const useBuilderAssistStreamMetadata = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  // note: url_base important here because path is built off of it
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_stream_metadata", hasRequiredParams, params, []);
};

export const useBuilderAssistStreamResponse = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_response_structure", hasRequiredParams, params, [
    "url_base",
  ]);
};

export interface BuilderAssistFindStreamsParams {
  enabled: boolean;
}
export interface BuilderAssistFoundStream {
  stream_name: string;
}
export interface BuilderAssistFindStreamsResponse extends BuilderAssistBaseResponse {
  streams: BuilderAssistFoundStream[];
}

export const useBuilderAssistFindStreams = (input: BuilderAssistFindStreamsParams) => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  const enabled = input.enabled && hasRequiredParams;
  return useAssistProxyQuery<BuilderAssistFindStreamsResponse>("find_streams", enabled, params, ["url_base"]);
};

export interface BuilderAssistCreateConnectorParams extends BuilderAssistCoreParams {
  stream_name: string;
  session_id: string;
}
export interface BuilderAssistCreateConnectorResponse extends BuilderAssistBaseResponse {
  connector: DeclarativeComponentSchema;
}

export const CONNECTOR_ASSIST_NOTIFICATION_ID = "connector-assist-notification";

export const useBuilderAssistCreateConnectorMutation = () => {
  const requestOptions = useRequestOptions();
  requestOptions.signal = AbortSignal.timeout(5 * 60 * 1000); // 5 minutes

  const { params: globalParams } = useAssistGlobalContext();

  const formatError = useFormatError();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (params: BuilderAssistCreateConnectorParams) => {
      const allParams = merge({}, globalParams, params);
      return explicitlyCastedAssistV1Process<BuilderAssistCreateConnectorResponse>(
        "create_connector",
        allParams,
        requestOptions
      );
    },
    {
      onError: (error: Error) => {
        const errorMessage = formatError(error);
        registerNotification({
          id: CONNECTOR_ASSIST_NOTIFICATION_ID,
          type: "error",
          text: errorMessage,
        });
      },
    }
  );
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
