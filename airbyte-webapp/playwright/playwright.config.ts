import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testIgnore: ["cloud/**", "**/cloud/**", "**/embedded/**", "embedded/**"],
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 4 : undefined,
  timeout: 60000, // 1 minute timeout for each test
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
    trace: process.env.CI ? "on-first-retry" : "on",
    screenshot: process.env.CI ? "only-on-failure" : "on",
    video: process.env.CI ? "on-first-retry" : "off",
  },
  webServer: {
    command: "cd .. && (test -d node_modules || pnpm install) && pnpm start local",
    port: 3000,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 120 * 1000, // 2 minutes timeout for webServer startup
  },

  /* Configure projects for major browsers */
  projects: [
    // Setup project
    { name: "setup", testMatch: /.*\.setup\.ts/ },

    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        // Use signed-in state.
        storageState: ".auth/user.json",
      },
      dependencies: ["setup"],
    },
  ],
});
