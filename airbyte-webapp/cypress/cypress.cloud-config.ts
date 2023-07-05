import { defineConfig } from "cypress";

export default defineConfig({
  e2e: {
    baseUrl: "https://localhost:3001/",
    specPattern: ["cypress/cloud-e2e/**/*.cy.ts"],
    supportFile: "cypress/support/cloud-e2e.ts",
  },
});
