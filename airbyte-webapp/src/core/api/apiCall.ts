import { trackError } from "core/utils/datadog";
import { shortUuid } from "core/utils/uuid";

import { CommonRequestError } from "./errors/CommonRequestError";
import { VersionError } from "./errors/VersionError";

export interface ApiCallOptions {
  getAccessToken: () => Promise<string | null>;
  signal?: RequestInit["signal"];
}

export interface RequestOptions<DataType = unknown> {
  url: string;
  method: "get" | "post" | "put" | "delete" | "patch";
  params?: Record<string, string | number | boolean>;
  data?: DataType;
  headers?: HeadersInit;
  responseType?: "blob";
  signal?: AbortSignal;
}

function getRequestBody<U>(data: U) {
  const stringifiedData = JSON.stringify(data);
  const nonJsonObject = stringifiedData === "{}";
  if (nonJsonObject) {
    // The app tries to stringify blobs which results in broken functionality.
    // There may be some edge cases where we pass in an empty object.
    return data as BodyInit;
  }
  return stringifiedData;
}

export const fetchApiCall = async <T, U = unknown>(
  { url, method, params, data, headers, responseType, signal }: RequestOptions<U>,
  options: ApiCallOptions,
  apiUrl: string
): Promise<typeof responseType extends "blob" ? Blob : T> => {
  // Remove the `v1/` in the end of the apiUrl for now, during the transition period
  // to get rid of it from all environment variables.
  const requestUrl = `${apiUrl.replace(/\/v1\/?$/, "")}${url.startsWith("/") ? "" : "/"}${url}`;

  const requestHeaders = new Headers(headers);
  const accessToken = await options.getAccessToken();
  if (accessToken) {
    requestHeaders.set("Authorization", `Bearer ${accessToken}`);
  }
  requestHeaders.set("X-Airbyte-Analytic-Source", "webapp");

  // We have a proper type for `params` in the RequestOptions interface, so types are validated correctly
  // when calling this method. Unfortunately the `URLSearchParams` typing in TS has wrong typings, since
  // it only allows for Record<string, string>, while the actual URLSearchParams API allow at least
  // Record<string, string | number | boolean> so we expect a compilation error here.
  // see https://github.com/microsoft/TypeScript/issues/32951
  // @ts-expect-error Due to the wrong TS types.
  const queryParams = new URLSearchParams(params).toString();
  const response = await fetch(`${requestUrl}${queryParams.length ? `?${queryParams}` : ""}`, {
    method,
    ...(data ? { body: getRequestBody(data) } : {}),
    headers: requestHeaders,
    signal: signal ?? options.signal,
  });

  /*
   * Orval only generates `responseType: "blob"` if the schema for an endpoint
   * is `type: string`, and `format: binary`.
   * If it references a type that is `type: string`, and `format: binary` it does not interpret
   * it correctly. So I am making an assumption that if it's not explicitly JSON, it's a binary file.
   */
  return parseResponse(response, requestUrl, responseType);
};

/** Parses response from server */
async function parseResponse<T>(response: Response, requestUrl: string, responseType?: "blob"): Promise<T> {
  if (response.status === 204) {
    return {} as T;
  }

  if (response.ok) {
    /*
     * Orval only generates `responseType: "blob"` if the schema for an endpoint
     * is `type: string, and format: binary`.
     * If it references a type that is `type: string, and format: binary` it does not interpret
     * it correct. So I am making an assumption that if it's not explicitly JSON, it's a binary file.
     */
    return responseType === "blob" || response.headers.get("content-type") !== "application/json"
      ? response.blob()
      : response.json();
  }

  if (response.headers.get("content-type") === "application/json") {
    const jsonError = await response.json();

    if (jsonError?.error?.startsWith("Version mismatch between")) {
      throw new VersionError(jsonError.error);
    }

    throw new CommonRequestError(response, jsonError);
  }

  let responseText: string | undefined;

  // Try to load the response body as text, since it wasn't JSON
  try {
    responseText = await response.text();
  } catch (e) {
    responseText = "<cannot load response body>";
  }

  const requestId = shortUuid();

  const error = new CommonRequestError(response, {
    message: `${
      response.status === 502 || response.status === 503 ? "Server temporarily unavailable" : "Unknown error"
    } (http.${response.status}.${requestId})`,
  });

  trackError(error, {
    httpStatus: response.status,
    httpUrl: requestUrl,
    httpBody: responseText,
    requestId,
  });
  console.error(`${requestUrl}: ${responseText} (http.${response.status}.${requestId})`);

  throw error;
}
