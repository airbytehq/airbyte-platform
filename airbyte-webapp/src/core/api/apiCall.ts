import { trackError } from "core/utils/datadog";

import { HttpError } from "./errors/HttpError";
import { HttpProblem } from "./errors/HttpProblem";
import { KnownApiProblem } from "./errors/problems";

export interface ApiCallOptions {
  getAccessToken: () => Promise<string | null>;
  signal?: RequestInit["signal"];
  includeCredentials?: boolean;
}

export interface RequestOptions<DataType = unknown> {
  url: string;
  method: "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
  params?: Record<string, string | number | boolean | string[]>;
  data?: DataType;
  headers?: HeadersInit;
  responseType?: "blob";
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
  request: RequestOptions<U>,
  options: ApiCallOptions,
  apiUrl: string
): Promise<typeof responseType extends "blob" ? Blob : T> => {
  const { url, method, params, data, headers, responseType } = request;
  const requestUrl = `${apiUrl}${url.startsWith("/") ? "" : "/"}${url}`;

  const requestHeaders = new Headers(headers);
  const accessToken = await options.getAccessToken();
  if (accessToken) {
    requestHeaders.set("Authorization", `Bearer ${accessToken}`);
  }
  requestHeaders.set("X-Airbyte-Analytic-Source", "webapp");

  // We have a proper type for `params` in the RequestOptions interface, so types are validated correctly
  // when calling this method. Unfortunately the `URLSearchParams` typing in TS has wrong typings, since
  // it only allows for Record<string, string>, while the actual URLSearchParams API allow at least
  // Record<string, string | number | boolean | string[]> so we expect a compilation error here.
  // see https://github.com/microsoft/TypeScript/issues/32951
  // @ts-expect-error Due to the wrong TS types.
  const queryParams = new URLSearchParams(params).toString();
  const response = await fetch(`${requestUrl}${queryParams.length ? `?${queryParams}` : ""}`, {
    method,
    ...(data ? { body: getRequestBody(data) } : {}),
    headers: requestHeaders,
    signal: options.signal,
    ...(options.includeCredentials ? { credentials: "include" } : {}),
  });

  return parseResponse(response, request, requestUrl, responseType);
};

/** Parses response from server */
async function parseResponse<T>(
  response: Response,
  request: RequestOptions,
  requestUrl: string,
  responseType?: "blob"
): Promise<T> {
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

  let responsePayload: unknown;
  try {
    responsePayload =
      response.headers.get("content-type") === "application/json" ? await response.json() : await response.text();
  } catch (e) {
    responsePayload = "<cannot load response body>";
  }

  // Create a HttpError or HttpProblem (if it has a defined response payload) for the request/response.
  // Replace the request url with the full url we called.
  const error = isKnownApiProblemResponse(responsePayload)
    ? new HttpProblem({ ...request, url: requestUrl }, response.status, responsePayload)
    : new HttpError({ ...request, url: requestUrl }, response.status, responsePayload);
  // Track HttpErrors here (instead of the error boundary), so we report all of them,
  // even the ones that will handled by our application via e.g. toast notification.
  trackError(error, { ...error });
  throw error;
}

/**
 * Check whether the payload of an error was a known API problem defined in api-problems.yaml.
 * We do a bit of a shortcut check here and only check if it has at least a `type` and `title` string property.
 * This is less accurate but faster than needing to try run through all possible error types.
 */
function isKnownApiProblemResponse(payload: unknown): payload is KnownApiProblem {
  return (
    !!payload &&
    typeof payload === "object" &&
    "type" in payload &&
    typeof payload.type === "string" &&
    "title" in payload &&
    typeof payload.title === "string"
  );
}
