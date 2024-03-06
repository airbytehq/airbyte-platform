import { config, MissingConfigError } from "core/config";
import { isCloudApp } from "core/utils/app";

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
  if (!isCloudApp() || !config.cloudApiUrl) {
    throw new MissingConfigError(`Can't fetch ${request.url}, because cloudApiUrl config isn't set.`);
  }
  return fetchApiCall<T>(request, options, config.cloudApiUrl);
};

/**
 * Execute a call against the Airbyte API (previously known as Cloud Public API).
 */
export const cloudAirbyteApiCall = async <T, U = unknown>(request: RequestOptions<U>, options: ApiCallOptions) => {
  if (!isCloudApp() || !config.cloudPublicApiUrl) {
    throw new MissingConfigError(`Can't fetch ${request.url}, because cloudPublicApiUrl config isn't set.`);
  }
  return fetchApiCall<T>(request, options, config.cloudPublicApiUrl);
};
