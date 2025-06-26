import { buildConfig } from "core/config";

import { ApiCallOptions, fetchApiCall, RequestOptions } from "./apiCall";

/**
 * Execute a call against the OSS api (airbyte-server).
 */
export const apiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  // NOTE: This will soon be updated on the BE and we will need to remove the /api from the url
  return fetchApiCall<T>(request, options, `${buildConfig.apiUrl}/api`);
};

/**
 * Execute a call against the connector builder API.
 */
export const connectorBuilderApiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  return fetchApiCall<T>(request, options, buildConfig.apiUrl);
};
