import type { Body, Op, Params, Reply } from "../routes";
import type { Store } from "../store";

/** A raw reply: an error status, or a body that is not the operation's typed response. */
export class MockResponse {
  readonly status: number;
  readonly json?: unknown;

  constructor(status: number, json?: unknown) {
    this.status = status;
    this.json = json;
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
