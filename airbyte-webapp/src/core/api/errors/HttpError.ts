import type { RequestOptions } from "../apiCall";

import { shortUuid } from "core/utils/uuid";

// Need to explicitally import from the file instead of core/errors to avoid circular dependencies
import { I18nError } from "../../errors/I18nError";

const defaultHttpMessage = (status: number) => {
  switch (status) {
    case 400:
      return "errors.http.badRequest";
    case 401:
      return "errors.http.unauthorized";
    case 403:
      return "errors.http.forbidden";
    case 404:
      return "errors.http.notFound";
    case 410:
      return "errors.http.gone";
    case 418:
      return "errors.http.teapot";
    case 422:
      return "errors.http.unprocessableEntity";
    case 429:
      return "errors.http.tooManyRequests";
    case 500:
      return "errors.http.internalServerError";
    case 502:
      return "errors.http.badGateway";
    case 503:
      return "errors.http.serviceUnavailable";
    default:
      return "errors.http.default";
  }
};

/**
 * HttpError represents a non-okay (i.e. 4xx/5xx) response from the server to an API call made.
 * It will contain information about the request, the HTTP status code, as well as the response payload.
 */
export class HttpError<PayloadType = unknown> extends I18nError {
  /**
   * A uniquely generated request ID for this error that will also be present on the
   * datadog error tracing of this error and can be used to find it.
   */
  public readonly requestId = shortUuid();
  constructor(
    /**
     * Information about the request that was made.
     */
    public readonly request: RequestOptions,
    /**
     * HTTP status code of the response.
     */
    public readonly status: number,
    /**
     * The response payload from the server. This could be parsed JSON if the server
     * returned it, or just a plain string e.g. in case of returned HTML.
     * The generic type of this class can be used to type this parameter for cases
     * the payload type is known.
     */
    public readonly response: PayloadType,
    /**
     * Optional i18nKey to overwrite default i18n logic of HttpError. Only meant to be used from inheritted classes.
     */
    i18nKey?: string,
    /**
     * Optional i18nParams to overwrite default i18n logic of HttpError. Only meant to be used from inheritted classes.
     */
    i18nParams?: I18nError["i18nParams"]
  ) {
    super(i18nKey ?? defaultHttpMessage(status), i18nParams ?? { status });
    this.name = "HttpError";
  }
}
