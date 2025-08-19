import { useMutation, useQuery } from "@tanstack/react-query";

import { DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM } from "components/connectorBuilder/constants";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { HttpError } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

import { resolveManifest, getHealthCheck } from "../generated/ConnectorBuilderClient";
import { KnownExceptionInfo } from "../generated/ConnectorBuilderClient.schemas";
import { ConnectorManifest, HealthCheckRead } from "../types/ConnectorBuilderClient";
import { DeclarativeComponentSchema } from "../types/ConnectorManifest";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const connectorBuilderKeys = {
  all: ["connectorBuilder"] as const,
  resolveSuspense: (manifest?: ConnectorManifest) =>
    [...connectorBuilderKeys.all, "resolveSuspense", { manifest }] as const,
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
      return DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM;
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
