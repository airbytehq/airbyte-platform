import type { Scenario } from "./scenario";
import type { Route } from "@playwright/test";

/**
 * How oss.airbyte.com/security-check answers. "closed", "open" and "default_auth" are the statuses the real service
 * sends. "fail" answers 500. "hang" never answers, so the check keeps running.
 */
export type SecurityCheckAnswer = "closed" | "open" | "default_auth" | "fail" | "hang";

interface ExternalHost {
  method: string;
  /** Origin and path, without the query string. A URL that ends with "/" matches every path below it. */
  url: string;
  answer(route: Route, scenario: Scenario): Promise<void>;
}

/**
 * Hosts outside Airbyte that the webapp calls, and how the mock answers each one. The answer comes from the scenario.
 * A request to any other external host fails the test.
 */
const externalHosts: ExternalHost[] = [
  {
    // The setup page asks this service whether the instance can be reached from the internet (security.ts).
    method: "POST",
    url: "https://oss.airbyte.com/security-check",
    answer: async (route, scenario) => {
      const answer = scenario.input.external?.securityCheck ?? "closed";
      if (answer === "hang") {
        return;
      }
      if (answer === "fail") {
        return route.fulfill({ status: 500, contentType: "text/plain", body: "Internal Server Error" });
      }
      return route.fulfill({ status: 200, json: { status: answer } });
    },
  },
  {
    // The code editor of the Connector Builder (Monaco) loads its files from this CDN. The URL is the default of
    // @monaco-editor/loader, which the webapp does not change. The mock does not answer these requests: it passes them
    // on to the next route of the test, or to the network.
    method: "GET",
    url: "https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/vs/",
    answer: (route) => route.fallback(),
  },
];

/** Finds the external host that answers a request, or `undefined` when the mock does not answer it. */
export function findExternalHost(method: string, url: URL) {
  const requested = `${url.origin}${url.pathname}`;
  return externalHosts.find(
    (host) =>
      host.method === method && (host.url.endsWith("/") ? requested.startsWith(host.url) : requested === host.url)
  );
}
