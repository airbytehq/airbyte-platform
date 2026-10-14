import type { MockAirbyte } from "./backend";
import type { Scenario } from "./scenario";

import { test as base } from "@playwright/test";

import { createMockBackend } from "./backend";
import environments from "../../environments.json";

/**
 * Origins that serve the Airbyte API during a test run: the webapp origin, the server host the live tests use,
 * and the API URL of the dev server. playwright.config.ts starts the dev server with `pnpm start local`.
 */
function apiOrigins(baseURL: string) {
  const urls = [baseURL, process.env.AIRBYTE_SERVER_HOST, process.env.REACT_APP_API_URL, environments.local.apiUrl];
  return [...new Set(urls.filter((url): url is string => !!url).map((url) => new URL(url).origin))];
}

interface MockOptions {
  /** Required. The backend state the test starts from. */
  airbyteScenario: Scenario | undefined;
}

/**
 * Playwright `test` with a mock Airbyte backend. Each test gets a fresh store built from its scenario.
 * Every API call is answered from the store. A request that no handler answers fails the test, with the
 * request listed as "METHOD path (operationId)".
 *
 * Set up data in the scenario. In the test, check the requests the webapp sent (`airbyte.calls`) and check
 * the page. Do not check the store.
 */
export const test = base.extend<{ airbyte: MockAirbyte } & MockOptions>({
  airbyteScenario: [undefined, { option: true }],
  storageState: { cookies: [], origins: [] },
  serviceWorkers: "block",

  airbyte: async ({ airbyteScenario, baseURL, context }, use, testInfo) => {
    if (!airbyteScenario) {
      throw new Error("Mocked tests need a scenario: test.use({ airbyteScenario: scenario({ ... }) })");
    }
    if (!baseURL) {
      throw new Error("Mocked tests need a baseURL");
    }

    const backend = createMockBackend(airbyteScenario, {
      appOrigin: new URL(baseURL).origin,
      apiOrigins: apiOrigins(baseURL),
    });
    await backend.install(context);

    await use(backend.airbyte);

    if (testInfo.status !== testInfo.expectedStatus || backend.problems().length > 0) {
      await testInfo.attach("mock-backend-report.json", {
        body: JSON.stringify(backend.report(), null, 2),
        contentType: "application/json",
      });
    }
    backend.verify();
  },

  // The mock must be installed before the page sends its first request, even when a test does not use `airbyte`.
  page: async ({ page, airbyte }, use) => {
    await use(page);
  },
});

export { expect } from "@playwright/test";
