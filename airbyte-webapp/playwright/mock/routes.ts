import type { RouteTypes } from "./generated/routes";

import { routes } from "./generated/routes";

/** An operationId of the Airbyte API, e.g. "webBackendUpdateConnection". Same name as the generated client function. */
export type Op = keyof RouteTypes & string;
/** Request body type of an operation (`undefined` when it sends no body). */
export type Body<O extends Op> = RouteTypes[O]["body"];
/** Path parameters of an operation, e.g. `{ sourceId: string }`. */
export type Params<O extends Op> = RouteTypes[O]["params"];
/** Response type of an operation (`void` when the server answers 204). */
export type Reply<O extends Op> = RouteTypes[O]["response"];

/** One entry of a route table: the HTTP method, the path, and the names of the `{params}` in the path. */
export interface RouteEntry {
  method: string;
  path: string;
  pathParams: readonly string[];
}

export interface MatchedRoute<Name extends string = Op> {
  op: Name;
  params: Record<string, string>;
}

/**
 * Makes a function that finds the operation for a request in a route table.
 * A path without params wins over a path with params, as in the server router.
 */
export function createRouteMatcher<Name extends string>(table: Record<Name, RouteEntry>) {
  const literalRoutes = new Map<string, Name>();
  const templatedRoutes: Array<{ method: string; pattern: RegExp; op: Name; names: readonly string[] }> = [];
  for (const [op, { method, path, pathParams }] of Object.entries(table) as Array<[Name, RouteEntry]>) {
    if (pathParams.length === 0) {
      literalRoutes.set(`${method} ${path}`, op);
    } else {
      const pattern = new RegExp(`^${path.replace(/\{\w+\}/g, "([^/]+)")}$`);
      templatedRoutes.push({ method, pattern, op, names: pathParams });
    }
  }

  return (method: string, pathname: string): MatchedRoute<Name> | undefined => {
    const literal = literalRoutes.get(`${method} ${pathname}`);
    if (literal) {
      return { op: literal, params: {} };
    }

    for (const route of templatedRoutes) {
      const match = route.method === method && pathname.match(route.pattern);
      if (match) {
        const params = Object.fromEntries(
          route.names.map((name, index) => [name, decodeURIComponent(match[index + 1])])
        );
        return { op: route.op, params };
      }
    }
    return undefined;
  };
}

/** Finds the Airbyte API operation for a request, or `undefined` when no operation has this method and path. */
export const matchRoute = createRouteMatcher<Op>(routes);
