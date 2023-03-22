import { config, MissingConfigError } from "config";

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

/**
 * Execute a call against the Cloud API (cloud-server)
 */
export const cloudApiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  if (!config.cloudApiUrl) {
    throw new MissingConfigError(`Can't fetch ${request.url}, because cloudApiUrl config isn't set.`);
  }
  return fetchApiCall<T>(request, options, config.cloudApiUrl);
};

/**
 * Execute a call against the Cloud Public API (aka Airbyte API).
 */
export const cloudPublicApiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  if (!config.cloudPublicApiUrl) {
    throw new MissingConfigError(`Can't fetch ${request.url}, because cloudPublicApiUrl config isn't set.`);
  }
  return fetchApiCall<T>(request, options, config.cloudPublicApiUrl);
};
