import type { MockAirbyte } from "./mockApi";

import { test as base, expect } from "@playwright/test";

import { createMockAirbyte } from "./mockApi";

export const test = base.extend<{ airbyte: MockAirbyte }>({
  storageState: { cookies: [], origins: [] },
  serviceWorkers: "block",
  // Playwright requires fixture dependencies to use an object pattern.
  // eslint-disable-next-line no-empty-pattern
  airbyte: async ({}, use) => {
    const airbyte = createMockAirbyte();
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
