import type { BaseProblemFields } from "@src/core/api/errors/problems";

import { MockResponse, notFound } from "./handlers/handler";

/**
 * Error replies in the formats that the real server, or the proxy in front of it, sends. Use them in an override or
 * in `airbyte.failAll`. For example: `airbyte.override("deleteConnection", () => errors.problem({ ... }))`.
 */
export const errors = {
  /** 404 for an unknown ID. The body is `{ errorId }` (IdNotFoundExceptionHandler.kt). */
  notFound,

  /**
   * A problem response (RFC 9457), as the server sends it for a thrown Problem (api-problems.yaml). The webapp turns a
   * body with `type` and `title` into an HttpProblem (apiCall.ts).
   */
  problem: (problem: BaseProblemFields & { status: number }) => new MockResponse(problem.status, problem),

  /** 502 from the proxy in front of the server, as when the server is down. The body is plain text, not JSON. */
  badGateway: () => MockResponse.text(502, "502 Bad Gateway"),
};
