/**
 * Shared setup for the mock backend self-tests in this folder. These tests do not load the webapp. A browser page
 * sends the requests that the webapp would send, and the test checks what the mock backend answers.
 */
import type { Scenario } from "../../mock";
import type { BrowserContext, Page } from "@playwright/test";

import { createMockBackend } from "../../mock/backend";

/** The path of the empty page that the self-tests open. The mock does not answer it. */
const SELF_TEST_PAGE = "/mock-self-test";

/**
 * Installs a mock backend for a scenario on the origin of baseURL. Then it opens an empty page on that origin, so
 * that the page can send requests to relative paths such as "/api/v1/connections/get", as the webapp does. The page
 * loads no app code.
 */
export async function installBackend(
  context: BrowserContext,
  page: Page,
  baseURL: string | undefined,
  input: Scenario
) {
  const appOrigin = new URL(baseURL!).origin;
  const backend = createMockBackend(input, { appOrigin, apiOrigins: [appOrigin] });
  await backend.install(context);
  await context.route(`${appOrigin}${SELF_TEST_PAGE}`, (route) =>
    route.fulfill({ contentType: "text/html", body: "" })
  );
  await page.goto(`${appOrigin}${SELF_TEST_PAGE}`);
  return backend;
}

/**
 * Sends one POST request with a JSON body from the page to the Airbyte API. `path` is the part after "/api/v1/",
 * such as "connections/sync". Returns the status and the parsed body. A 204 reply has no body.
 */
export function post(page: Page, path: string, body: object) {
  return page.evaluate(
    async ({ path, body }) => {
      const response = await fetch(`/api/v1/${path}`, { method: "POST", body: JSON.stringify(body) });
      return { status: response.status, body: response.status === 204 ? undefined : await response.json() };
    },
    { path, body }
  );
}
