import { defineConfig } from "cypress";

export default defineConfig({
  viewportHeight: 800,
  viewportWidth: 1280,
  pageLoadTimeout: 10000,
  e2e: {
    baseUrl: "https://localhost:3001/",
    specPattern: ["cypress/cloud-e2e/**/*.cy.ts"],
    supportFile: "cypress/support/cloud-e2e.ts",
    env: {
      AIRBYTE_TEST_WORKSPACE_ID: "f1ba662c-4135-457c-99fe-6e3a175924fe",
      LOGIN_URL: "https://frontend-dev-cloud.airbyte.com",
    },
  },
});
