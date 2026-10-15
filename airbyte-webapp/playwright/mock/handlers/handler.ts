import type { Body, Op, Params, Reply } from "../routes";
import type { Store } from "../store";

/** A raw reply: an error status, or a body that is not the operation's typed response. */
export class MockResponse {
  readonly status: number;
  readonly json?: unknown;
  /** A body that is not JSON, such as an error page of a proxy. The mock sends it as text/plain instead of `json`. */
  readonly text?: string;

  constructor(status: number, json?: unknown, text?: string) {
    this.status = status;
    this.json = json;
    this.text = text;
  }

  /** A reply with a text/plain body instead of JSON. */
  static text(status: number, body: string): MockResponse {
    return new MockResponse(status, undefined, body);
  }
}

/** The server's answer for an unknown ID (IdNotFoundExceptionHandler.kt). */
export function notFound(): MockResponse {
  return new MockResponse(404, { errorId: "00000000-0000-4000-8000-000000000404" });
}

export interface HandlerContext<O extends Op> {
  body: Body<O>;
  params: Params<O>;
  store: Store;
}

/**
 * Answers one operation from the store. Return the typed reply (or `undefined` for a 204 operation),
 * or a MockResponse for errors.
 *
 * A handler only saves and returns data. It does not copy server logic, such as deciding whether a schema change
 * breaks a connection. Tests check requests with `airbyte.calls`, so handlers contain no `expect()`.
 */
export type Handler<O extends Op> = (context: HandlerContext<O>) => Reply<O> | MockResponse;

export type HandlerMap = { [O in Op]?: Handler<O> };

export interface OverrideContext<O extends Op> extends HandlerContext<O> {
  /** Runs the default handler of the operation and returns its reply. Throws when the operation has no default handler. */
  next(): Reply<O> | MockResponse;
}

/**
 * Answers one operation in place of its default handler, for the rest of one test. Set it with `airbyte.override`.
 * Use it for an answer that the default handler does not give: an error, or a backend decision that the mock does not
 * compute.
 */
export type Override<O extends Op> = (context: OverrideContext<O>) => Reply<O> | MockResponse;
