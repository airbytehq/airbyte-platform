import { useMutation, useQuery } from "@tanstack/react-query";
import merge from "lodash/merge";
import omit from "lodash/omit";

import { HttpError } from "core/api";
import { useFormatError } from "core/errors";
import { useDebounceValue } from "core/utils/useDebounceValue";
import { useNotificationService } from "hooks/services/Notification";

import { ApiCallOptions } from "../apiCall";
import { assistV1Process } from "../generated/AirbyteClient";
import { AssistV1ProcessRequestBody, KnownExceptionInfo } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const OMIT_ASSIST_KEYS_IN_CACHE = ["manifest"]; // just extra metadata that we don't need to factor into cache

const connectorBuilderAssistKeys = {
  all: ["connectorBuilderAssist"] as const,
  assist: (controller: string, params: AssistV1ProcessRequestBody, ignoreCacheKeys: string[]) =>
    [
      ...connectorBuilderAssistKeys.all,
      "assist",
      controller,
      JSON.stringify(omit(params, ...OMIT_ASSIST_KEYS_IN_CACHE.concat(ignoreCacheKeys))),
    ] as const,
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

  const queryKey = connectorBuilderAssistKeys.assist(controller, params, ignoreCacheKeys);
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
