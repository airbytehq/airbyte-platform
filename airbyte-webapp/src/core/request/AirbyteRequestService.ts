import merge from "lodash/merge";

import { trackError } from "core/utils/datadog";
import { shortUuid } from "core/utils/uuid";

import { CommonRequestError } from "./CommonRequestError";
import { RequestMiddleware } from "./RequestMiddleware";
import { VersionError } from "./VersionError";
import { ApiCallOptions } from "../api";

/**
 * @deprecated This class will be removed soon and should no longer be used or extended.
 */
abstract class AirbyteRequestService {
  private readonly rootUrl: string;

  constructor(rootUrl: string, private middlewares: RequestMiddleware[] = []) {
    // Remove the `/v1/` at the end of the URL if it exists, during the transition period
    // to remove it from all cloud environments
    this.rootUrl = rootUrl.replace(/\/v1\/?$/, "");
  }

  protected get requestOptions(): ApiCallOptions {
    return {
      middlewares: this.middlewares,
    };
  }

  /** Perform network request */
  public async fetch<T = Response>(url: string, body?: unknown, options?: Partial<RequestInit>): Promise<T> {
    const path = `${this.rootUrl}${url.startsWith("/") ? "" : "/"}${url}`;

    const requestOptions: RequestInit = merge(
      {
        method: "POST",
        body: body ? JSON.stringify(body) : undefined,
        headers: {
          "Content-Type": "application/json",
          "X-Airbyte-Analytic-Source": "webapp",
        },
      },
      options
    );

    let preparedOptions: RequestInit = requestOptions;

    for (const middleware of this.middlewares) {
      preparedOptions = await middleware(preparedOptions);
    }
    const response = await fetch(path, preparedOptions);

    return parseResponse(response, path);
  }
}

/** Parses errors from server */
async function parseResponse<T>(response: Response, requestUrl: string): Promise<T> {
  if (response.status === 204) {
    return {} as T;
  }
  if (response.status >= 200 && response.status < 300) {
    const contentType = response.headers.get("content-type");

    if (contentType === "application/json") {
      return await response.json();
    }

    // @ts-expect-error TODO: needs refactoring of services
    return response;
  }

  if (response.headers.get("content-type") === "application/json") {
    const jsonError = await response.json();

    if (jsonError?.error?.startsWith("Version mismatch between")) {
      throw new VersionError(jsonError.error);
    }

    throw new CommonRequestError(response, jsonError?.message ?? JSON.stringify(jsonError?.detail));
  }

  let responseText: string | undefined;

  // Try to load the response body as text, since it wasn't JSON
  try {
    responseText = await response.text();
  } catch (e) {
    responseText = "<cannot load response body>";
  }

  const requestId = shortUuid();

  const error = new CommonRequestError(
    response,
    `${response.status === 502 || response.status === 503 ? "Server temporarily unavailable" : "Unknown error"} (http.${
      response.status
    }.${requestId})`
  );

  trackError(error, {
    httpStatus: response.status,
    httpUrl: requestUrl,
    httpBody: responseText,
    requestId,
  });
  console.error(`${requestUrl}: ${responseText} (http.${response.status}.${requestId})`);

  throw error;
}

export { AirbyteRequestService };
