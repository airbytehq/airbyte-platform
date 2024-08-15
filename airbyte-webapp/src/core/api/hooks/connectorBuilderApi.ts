import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { resolveAndValidate } from "components/connectorBuilder/Builder/manifestHelpers";
import { ManifestValidationError } from "components/connectorBuilder/utils";

import { HttpError } from "core/api";
import { useFormatError } from "core/errors";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";

import { readStream, generateContribution, checkContribution } from "../generated/ConnectorBuilderClient";
import {
  ConnectorConfig,
  StreamRead,
  StreamReadRequestBody,
  GenerateContributionRequestBody,
  CheckContributionRead,
  CheckContributionRequestBody,
} from "../types/ConnectorBuilderClient";
import { ConnectorManifest } from "../types/ConnectorManifest";
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

// use react-query for the async resolveAndValidate call, even though it isn't an API call,
// to gain caching and suspense support out of the box
export const useBuilderResolvedManifest = (manifest: ConnectorManifest, enabled = true) => {
  return useQuery<ConnectorManifest, ManifestValidationError>(
    connectorBuilderKeys.resolve(manifest),
    () => resolveAndValidate(manifest),
    {
      keepPreviousData: true,
      retry: false,
      enabled,
    }
  );
};

export const useBuilderResolvedManifestSuspense = (manifest: ConnectorManifest) => {
  return useSuspenseQuery(connectorBuilderKeys.resolveSuspense(manifest), async () => {
    try {
      return await resolveAndValidate(manifest);
    } catch {
      return null;
    }
  });
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
