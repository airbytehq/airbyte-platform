import type { MockAirbyte, MockAirbyteScenario } from "./mockApi";

import { test as base, expect } from "@playwright/test";

import { createMockAirbyte } from "./mockApi";

export const test = base.extend<{ airbyte: MockAirbyte; airbyteScenario: MockAirbyteScenario | undefined }>({
  airbyteScenario: [undefined, { option: true }],
  storageState: { cookies: [], origins: [] },
  serviceWorkers: "block",
  airbyte: async ({ airbyteScenario }, use) => {
    if (!airbyteScenario) {
      throw new Error("Frontend acceptance tests require an airbyteScenario supplied through test.use()");
    }

    const airbyte = createMockAirbyte(airbyteScenario);
    await use(airbyte);
    airbyte.assertNoUnexpectedRequests();
  },
  context: async ({ context, airbyte, baseURL }, use) => {
    if (!baseURL) {
      throw new Error("Frontend acceptance tests require a baseURL");
    }
    await airbyte.install(context, baseURL);
    await use(context);
  },
});

export { expect };
