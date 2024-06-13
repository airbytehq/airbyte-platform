import { defineConfig } from "cypress";

export default defineConfig({
  viewportHeight: 800,
  viewportWidth: 1280,
  pageLoadTimeout: 10000,
  e2e: {
    baseUrl: "https://localhost:3001/",
    specPattern: ["cypress/cloud-e2e/**/*.cy.ts"],
    supportFile: "cypress/support/cloud-e2e.ts",
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    setupNodeEvents: require("dd-trace/ci/cypress/plugin"),
    env: {
      LOGIN_URL: "https://frontend-dev-cloud.airbyte.com",
    },
  },
  retries: {
    runMode: 5,
    openMode: 0,
  },
});
