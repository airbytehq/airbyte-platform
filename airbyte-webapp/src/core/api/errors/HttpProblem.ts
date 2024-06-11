import type { KnownApiProblem } from "./problems";
import type { RequestOptions } from "../apiCall";

import { HttpError } from "./HttpError";

/**
 * An `HttpProblem` is a special kind of `HttpError` that represents an error with a known response body.
 * The central fetching logic will create this error for non 200 error cases instead of `HttpError` if the
 * response body is following our common error scheme of responses.
 */
export class HttpProblem<PayloadType extends KnownApiProblem = KnownApiProblem> extends HttpError<PayloadType> {
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
    public readonly response: PayloadType
  ) {
    // TODO: Pass better i18nKey/params here, once we have a better error translation system in place
    super(request, status, response);
    this.name = "HttpProblem";
  }

  /**
   * Check if a given error is an instance of HttpProblem.
   * Always prefer this method over `error instanceof HttpProblem`. This utitlity methods makes sure
   * you get the correct generic type for HttpProblem back, so that `error.response` will
   * be of type `KnownApiProblem`.
   */
  static isInstanceOf(error: unknown): error is HttpProblem<KnownApiProblem> {
    return error instanceof HttpProblem;
  }

  /**
   * Check if an error is a known HttpProblem with a specific type.
   */
  static isType<T extends KnownApiProblem["type"]>(
    error: unknown,
    type: T
  ): error is HttpProblem<Extract<KnownApiProblem, { type: T }>> {
    return HttpProblem.isInstanceOf(error) && error.response.type === type;
  }

  /**
   * Check if an error is a known HttpProblem with the type being either the exact passed type of a hierarchical subtype of it.
   * e.g. passing error:validation will match error:validation or error:valiation/invalid-email.
   */
  static isTypeOrSubtype(error: unknown, type: string): error is HttpProblem<KnownApiProblem> {
    return (
      HttpProblem.isInstanceOf(error) && (error.response.type === type || error.response.type.startsWith(`${type}/`))
    );
  }
}
