import type { KnownApiProblem, KnownApiProblemTypeAndPrefixes } from "./problems";
import type { RequestOptions } from "../apiCall";

import { FormatMessageParams } from "core/errors/I18nError";
import errorMessages from "locales/en.errors.json";

import { HttpError } from "./HttpError";

// Generalizing the type, so TypeScript won't do strict compile time checks, since we do runtime checks for availability of messages in this object.
const messages = errorMessages as Record<string, string>;

type TranslationType = "exact" | "hierarchical" | "detail" | "title";

function getTranslation(response: KnownApiProblem): {
  key: string;
  params?: FormatMessageParams;
  type: TranslationType;
} {
  const match = response.type.match(/^error:(?<error>.*)$/);
  const isHierarchicalType = Boolean(match && match.groups?.error);
  const errorType = match?.groups?.error || response.type;
  if (messages[errorType]) {
    // If we have an exact match on the type or in case if it's a hierarchical type only on the part behind "error:" use this message.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { key: `error:${errorType}`, params: response.data as any, type: "exact" };
  }

  if (isHierarchicalType) {
    // If we have a hierarchical type, try to see if any parent error type has a message available.
    const hierarchy = errorType.split("/");
    for (let i = hierarchy.length - 1; i > 0; i--) {
      const parentType = hierarchy.slice(0, i).join("/");
      if (messages[parentType]) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return { key: `error:${parentType}`, params: response.data as any, type: "hierarchical" };
      }
    }
  }

  if (response.detail) {
    // If the error has a detail on it, use that as the message.
    return { key: "errors.messageOnly", params: { message: response.detail }, type: "detail" };
  }

  // In all other cases use the title as the message.
  return { key: "errors.messageOnly", params: { message: response.title }, type: "title" };
}

/**
 * An `HttpProblem` is a special kind of `HttpError` that represents an error with a known response body.
 * The central fetching logic will create this error for non 200 error cases instead of `HttpError` if the
 * response body is following our common error scheme of responses.
 */
export class HttpProblem<PayloadType extends KnownApiProblem = KnownApiProblem> extends HttpError<PayloadType> {
  /**
   * The way how the error message found it's translation:
   * - `exact`: The translation was found as an exact match in the translation file.
   * - `hirarchical`: The translation was found as a parent type in the translation file.
   * - `detail`: The translation was picked from `detail` in the response.
   * - `title`: The translation was picked from `title` in the response.
   */
  public readonly i18nType: TranslationType;

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
    const { key, params, type } = getTranslation(response);
    super(request, status, response, key, params);
    this.name = "HttpProblem";
    this.i18nType = type;
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
  static isTypeOrSubtype(error: unknown, type: KnownApiProblemTypeAndPrefixes): error is HttpProblem<KnownApiProblem> {
    return (
      HttpProblem.isInstanceOf(error) && (error.response.type === type || error.response.type.startsWith(`${type}/`))
    );
  }
}
