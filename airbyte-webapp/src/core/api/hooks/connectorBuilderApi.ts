import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import merge from "lodash/merge";
import omit from "lodash/omit";
import { useCallback } from "react";

import {
  DEFAULT_JSON_MANIFEST_VALUES,
  DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM,
} from "components/connectorBuilder/constants";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { HttpError } from "core/api";
import { useFormatError } from "core/errors";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useDebounceValue } from "core/utils/useDebounceValue";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";

import { ApiCallOptions } from "../apiCall";
import {
  assistV1Process,
  readStream,
  resolveManifest,
  generateContribution,
  checkContribution,
  getHealthCheck,
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
  HealthCheckRead,
} from "../types/ConnectorBuilderClient";
import { DeclarativeComponentSchema } from "../types/ConnectorManifest";
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
  assist: (controller: string, params: AssistV1ProcessRequestBody, ignoreCacheKeys: string[]) =>
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
  const [advancedMode] = useLocalStorage("airbyte_connector-builder-advanced-mode", false);
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  return useSuspenseQuery(connectorBuilderKeys.resolveSuspense(manifest), async () => {
    if (!manifest) {
      return isSchemaFormEnabled && advancedMode
        ? DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM
        : DEFAULT_JSON_MANIFEST_VALUES;
    }
    try {
      return (await resolveManifestQuery(manifest, projectId)).manifest as DeclarativeComponentSchema;
    } catch {
      return null;
    }
  });
};

export const useResolveManifest = () => {
  const workspaceId = useCurrentWorkspaceId();
  const requestOptions = useRequestOptions();

  const mutation = useMutation(
    ({ manifestToResolve, projectId }: { manifestToResolve: DeclarativeComponentSchema; projectId?: string }) => {
      return resolveManifest(
        {
          manifest: {
            ...manifestToResolve,
            // normalize the manifest in the CDK to produce properly linked fields and parent stream references
            __should_normalize: true,
            __should_migrate: true,
          },
          workspace_id: workspaceId,
          project_id: projectId,
          form_generated_manifest: false,
        },
        requestOptions
      );
    }
  );

  return {
    resolveManifest: mutation.mutateAsync, // Returns a promise that resolves with the result or rejects with error
    isResolving: mutation.isLoading,
    resolveError: mutation.error as HttpError<KnownExceptionInfo> | null,
    resetResolveState: mutation.reset,
  };
};

export const explicitlyCastedAssistV1Process = <T>(
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

export const useAssistApiProxyQuery = <T>(
  controller: string,
  enabled: boolean,
  params: AssistV1ProcessRequestBody,
  ignoreCacheKeys: string[],
  transformResult?: (data: T) => T
) => {
  const requestOptions = useRequestOptions();
  requestOptions.signal = AbortSignal.timeout(5 * 60 * 1000); // 5 minutes

  const queryKey = connectorBuilderKeys.assist(controller, params, ignoreCacheKeys);
  const debouncedQueryKey = useDebounceValue(queryKey, 500);

  return useQuery<T, HttpError<KnownExceptionInfo>>(
    debouncedQueryKey,
    async () => {
      const result = await explicitlyCastedAssistV1Process<T>(controller, params, requestOptions);
      return transformResult ? transformResult(result) : result;
    },
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

export const CONNECTOR_ASSIST_NOTIFICATION_ID = "connector-assist-notification";

export const useAssistApiMutation = <T extends AssistV1ProcessRequestBody, U>(
  globalParams: AssistV1ProcessRequestBody
) => {
  const requestOptions = useRequestOptions();
  requestOptions.signal = AbortSignal.timeout(5 * 60 * 1000); // 5 minutes

  const formatError = useFormatError();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (params: T) => {
      const allParams = merge({}, globalParams, params);
      return explicitlyCastedAssistV1Process<U>("create_connector", allParams, requestOptions);
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

export const useBuilderHealthCheck = () => {
  const requestOptions = useRequestOptions();
  return useQuery<HealthCheckRead, HttpError<KnownExceptionInfo>>(["builderHealthCheck"], () =>
    getHealthCheck(requestOptions)
  );
};

/**
 * Hook to check if custom components are enabled.
 * Returns true if either the feature flag is enabled or the global override is set.
 *
 * PROBLEM TO WORKAROUND: We do not have the ability to set feature flags for OSS/Enterprise customers.
 *
 * HACK: We in stead use a global override from the server in the form of an environment variable AIRBYTE_ENABLE_UNSAFE_CODE.
 *       Any customer can set this environment variable to enable unsafe code execution.
 *
 * TODO: Remove this when we have the ability to set feature flags for OSS/Enterprise customers.
 * @returns boolean indicating if custom components are enabled
 */
export const useCustomComponentsEnabled = () => {
  const areCustomComponentsEnabled = useExperiment("connectorBuilder.customComponents");
  const { data } = useBuilderHealthCheck();
  return areCustomComponentsEnabled || data?.capabilities?.custom_code_execution;
};
