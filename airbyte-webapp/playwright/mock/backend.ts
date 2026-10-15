import type { ConnectionHandle } from "./builders/connection";
import type { Override } from "./handlers/handler";
import type { Recorder } from "./recorder";
import type { Op } from "./routes";
import type { Scenario } from "./scenario";
import type { SchemaRefreshAnswer, Store } from "./store";
import type { BrowserContext, Route } from "@playwright/test";

import { findExternalHost } from "./external";
import { defaultHandlers } from "./handlers";
import { MockResponse } from "./handlers/handler";
import { createRecorder } from "./recorder";
import { matchRoute } from "./routes";
import { createStore } from "./store";

export type ConnectionTab = "status" | "timeline" | "replication" | "settings" | "mappings";

/** The `airbyte` fixture a test receives. */
export interface MockAirbyte {
  readonly scenario: Scenario;
  readonly store: Store;
  readonly calls: Recorder;
  /** ID of the scenario's workspace. */
  readonly workspaceId: string;
  /**
   * Replaces the answer of one operation for the rest of the test. Call it again to change the answer during the test.
   * The handler gets the context of a default handler, plus `next()`, which runs the default handler. Example of a
   * backend reaction to a save:
   *
   *   airbyte.override("webBackendUpdateConnection", ({ body, next, store }) => {
   *     next();
   *     return store.connections.update(body.connectionId, { schemaChange: "no_change" });
   *   });
   */
  override<O extends Op>(op: O, handler: Override<O>): void;
  /**
   * From now on, every Airbyte API call answers `response`, as when the server is down. It wins over overrides.
   * External hosts still answer.
   */
  failAll(response: MockResponse): void;
  /** Answers that the mock cannot compute for schema actions, such as a schema refresh. */
  readonly schema: {
    /**
     * Sets what the next schema refreshes of a connection find. The webapp refreshes in one of two ways, chosen by the
     * asyncSchemaDiscovery experiment (ConnectionEditService.tsx). The mock answers the synchronous way only, so the
     * scenario must set `experiments: { asyncSchemaDiscovery: false }`.
     */
    onRefresh(connection: ConnectionHandle | string, answer: SchemaRefreshAnswer): void;
  };
  /** URLs of app pages, relative to baseURL. */
  readonly paths: {
    connection(ref: ConnectionHandle | string, tab?: ConnectionTab): string;
  };
}

export interface MockBackendOptions {
  /** Origin of the webapp (Playwright baseURL). Requests to it that are not API calls pass through. */
  appOrigin: string;
  /** Origins that serve the Airbyte API. Requests to `/api/` on these origins go to the mock. */
  apiOrigins: string[];
}

/** Describes a request as "METHOD path (operationId)". Requests to other hosts show the full URL. */
function describeRequest(method: string, url: URL, isApiCall: boolean, op: Op | undefined) {
  if (!isApiCall) {
    return `${method} ${url.origin}${url.pathname} (external host)`;
  }
  return `${method} ${url.pathname} (${op ?? "unknown operation"})`;
}

/**
 * A handler or override without the types of its operation. TypeScript checks each handler and override against the
 * types of its own operation. The operation is known only when a request arrives, so TypeScript cannot check the call.
 */
type UntypedHandler = (context: unknown) => unknown;

export function createMockBackend(scenario: Scenario, options: MockBackendOptions) {
  const { appOrigin, apiOrigins } = options;
  const store = createStore(scenario);
  const { calls, record } = createRecorder();
  const unexpected: string[] = [];
  const handlerErrors: string[] = [];
  const notFound: string[] = [];
  const overrides = new Map<Op, UntypedHandler>();
  let allCallsFail: MockResponse | undefined;

  const isApiCall = (url: URL) => apiOrigins.includes(url.origin) && url.pathname.startsWith("/api/");

  async function answer(route: Route) {
    const request = route.request();
    const url = new URL(request.url());
    const apiCall = isApiCall(url);
    const externalHost = apiCall ? undefined : findExternalHost(request.method(), url);
    if (externalHost) {
      return externalHost.answer(route, scenario);
    }

    const matched = apiCall ? matchRoute(request.method(), url.pathname) : undefined;
    const description = describeRequest(request.method(), url, apiCall, matched?.op);

    const handler = matched && (defaultHandlers[matched.op] as UntypedHandler | undefined);
    const override = matched && overrides.get(matched.op);
    const hasAnswer = Boolean(allCallsFail || override || handler);
    if (!matched || !hasAnswer) {
      unexpected.push(description);
      return route.abort("failed");
    }

    let body: unknown;
    let reply: unknown;
    try {
      // postDataJSON() throws when the body is not JSON. Inside the try, that shows as a handler error.
      body = request.postData() ? request.postDataJSON() : undefined;
      const context = { body, params: matched.params, store };
      const runDefault = () => {
        if (!handler) {
          throw new Error(`The override called next(), but the mock has no default handler for ${matched.op}`);
        }
        return handler(context);
      };

      // The answer comes from the first of: failAll, the override of the operation, the default handler.
      if (allCallsFail) {
        reply = allCallsFail;
      } else if (override) {
        reply = override({ ...context, next: runDefault });
      } else {
        reply = runDefault();
      }
    } catch (error) {
      handlerErrors.push(`${description}: ${error instanceof Error ? error.message : String(error)}`);
      return route.fulfill({ status: 500, json: { message: "Mock handler error" } });
    }

    const response = reply instanceof MockResponse ? reply : undefined;
    const status = response?.status ?? (reply === undefined ? 204 : 200);
    record({ op: matched.op, method: request.method(), path: url.pathname, body, status });
    if (status === 404) {
      notFound.push(description);
    }

    if (status === 204) {
      return route.fulfill({ status });
    }
    if (response?.text !== undefined) {
      return route.fulfill({ status, contentType: "text/plain", body: response.text });
    }
    return route.fulfill({ status, json: response ? response.json : reply });
  }

  /** Routes every API call and every request to another origin. Same-origin assets pass through untouched. */
  async function install(context: BrowserContext) {
    const { experiments = {}, features = {} } = scenario.input;
    await context.addInitScript(
      ({ experiments, features }) => {
        // Always set the E2E flag. Without it, the dev server uses the experiment values in .experiments.dev
        // (ExperimentService.tsx) and ignores the feature overrides (FeatureService.tsx).
        window._e2ePlaywrightEnvironment = true;
        window._e2eOverwrites = experiments;
        window._e2eFeatureOverwrites = features;
      },
      { experiments, features }
    );
    await context.route((url) => isApiCall(url) || url.origin !== appOrigin, answer);
  }

  /** Requests the mock did not answer, and handlers that threw. Each one fails the test. */
  function problems() {
    return [
      ...unexpected.map((line) => `Unexpected request: ${line}`),
      ...handlerErrors.map((line) => `Handler error: ${line}`),
    ];
  }

  /** Throws when the webapp sent a request the mock does not answer, or a handler threw. */
  function verify() {
    const lines = problems();
    if (lines.length > 0) {
      throw new Error(`The mock backend got requests that the test did not expect:\n  ${lines.join("\n  ")}`);
    }
  }

  /** Report for a failed test: every answered call and every 404 the mock sent. */
  function report() {
    return { calls: calls.log(), notFound: [...notFound], unexpected: [...unexpected] };
  }

  const workspaceId = store.identity.workspace.workspaceId;
  const airbyte: MockAirbyte = {
    scenario,
    store,
    calls,
    workspaceId,
    override: (op, handler) => {
      overrides.set(op, handler as UntypedHandler);
    },
    failAll: (response) => {
      allCallsFail = response;
    },
    schema: {
      onRefresh: (connection, answer) => {
        if (scenario.input.experiments?.asyncSchemaDiscovery !== false) {
          throw new Error(
            "airbyte.schema.onRefresh answers the synchronous refresh only. " +
              "Set experiments: { asyncSchemaDiscovery: false } in the scenario."
          );
        }
        store.schemaRefreshes.set(connection, answer);
      },
    },
    paths: {
      connection: (ref, tab) => {
        const connectionId = typeof ref === "string" ? ref : store.connections.get(ref).connectionId;
        return `/workspaces/${workspaceId}/connections/${connectionId}${tab ? `/${tab}` : ""}`;
      },
    },
  };

  return { airbyte, install, problems, verify, report };
}
