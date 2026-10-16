import { defineConfig, devices } from "@playwright/test";

// The fe-e2e-playwright-test CI job sets PLAYWRIGHT_PRODUCTION_BUILD=true to test a production build of the webapp.
// The dev server compiles each module on request, which makes every page load slow.
const productionBuild = process.env.PLAYWRIGHT_PRODUCTION_BUILD === "true";
// The backend URLs that `pnpm start local` gives to Vite (scripts/start-dev.js, environments.json).
const localUrls = {
  REACT_APP_API_URL: process.env.REACT_APP_API_URL ?? "https://local.airbyte.dev",
  REACT_APP_KEYCLOAK_BASE_URL: "https://local.airbyte.dev",
};

export default defineConfig({
  testDir: "./tests",
  testIgnore: ["cloud/**", "**/cloud/**"],
  // Writes the route table of the mock backend. It runs after the dev server has generated the API client.
  globalSetup: "./scripts/generate-mock-routes.js",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.PLAYWRIGHT_WORKERS ? Number(process.env.PLAYWRIGHT_WORKERS) : process.env.CI ? 2 : undefined,
  timeout: 120000, // 2 minute timeout for each test
  reporter: process.env.CI
    ? [
        ["html"],
        ["json", { outputFile: "playwright-report/report.json" }],
        ["junit", { outputFile: "playwright-report/junit.xml" }],
      ]
    : "html",
  use: {
    baseURL: "https://localhost:3000",
    ignoreHTTPSErrors: true,
    trace: process.env.CI ? "retain-on-failure" : "on",
    screenshot: process.env.CI ? "only-on-failure" : "on",
  },
  webServer: {
    // `pnpm build` generates the API client like `pnpm start local` does, then runs `vite build`.
    command: productionBuild
      ? "cd .. && (test -d node_modules || pnpm install) && pnpm build && pnpm vite preview --port 3000 --strictPort"
      : "cd .. && (test -d node_modules || pnpm install) && pnpm start local",
    port: 3000,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
    // The production build takes about 2 minutes in CI. The dev server starts in less.
    timeout: (productionBuild ? 5 : 2) * 60 * 1000,
    // CI loads the Datadog test tracer with NODE_OPTIONS. Only the tests need it, not the dev server.
    env: { NODE_OPTIONS: "", ...(productionBuild ? localUrls : {}) },
  },

  /* Configure projects for major browsers */
  projects: [
    // OSS Acceptance tests
    {
      name: "oss-acceptance",
      use: { ...devices["Desktop Chrome"] },
      // Must include top-level testIgnore patterns plus schema exclusion
      testIgnore: ["cloud/**", "**/cloud/**"],
    },
  ],
});
