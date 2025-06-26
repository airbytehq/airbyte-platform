import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: ["cloud/**", "**/cloud/**"],
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: "html",
  use: {
    baseURL: process.env.AIRBYTE_WEBAPP_URL ?? process.env.AIRBYTE_SERVER_HOST,
    ignoreHTTPSErrors: true,
    trace: "on", // always collect trace for playback
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
