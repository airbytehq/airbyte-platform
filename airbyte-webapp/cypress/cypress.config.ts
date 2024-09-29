import { defineConfig } from "cypress";
import pg from "pg-promise";

const pgp = pg();

export const DB_CONFIG = {
  user: process.env.SOURCE_DB_USER || "postgres",
  host: process.env.SOURCE_DB_HOST || "127.0.0.1",
  database: process.env.SOURCE_DB_NAME || "airbyte_ci_source",
  password: process.env.SOURCE_DB_PASSWORD || "secret_password",
  port: Number(process.env.SOURCE_DB_PORT || 5433),
};

export default defineConfig({
  projectId: "916nvw",
  viewportHeight: 800,
  viewportWidth: 1280,
  e2e: {
    baseUrl: "https://localhost:3000",
    specPattern: ["cypress/e2e/**/*.cy.ts"],
    supportFile: "cypress/support/e2e.ts",
    setupNodeEvents(on, config) {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      require("dd-trace/ci/cypress/plugin")(on, config);
      on("task", {
        dbQuery(params: { query: string }) {
          const { query } = params;
          const db = pgp(DB_CONFIG);
          return db.any(query).finally(db.$pool.end);
        },
      });

      on("before:browser:launch", (browser, launchOptions) => {
        if (browser.family === "chromium" && browser.name !== "electron") {
          launchOptions.args.push("--incognito");
        }
        return launchOptions;
      });
    },
  },
  retries: {
    runMode: 5,
    openMode: 0,
  },
  defaultCommandTimeout: 10000,
});
