import { config } from "core/config";

import { ApiCallOptions, fetchApiCall, RequestOptions } from "./apiCall";

/**
 * Execute a call against the OSS api (airbyte-server).
 */
export const apiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  return fetchApiCall<T>(request, options, config.apiUrl);
};

/**
 * Execute a call against the connector builder API.
 */
export const connectorBuilderApiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  return fetchApiCall<T>(request, options, config.connectorBuilderApiUrl);
};
