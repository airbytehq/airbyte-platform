import { defineConfig } from "cypress";
import pg from "pg-promise";

const pgp = pg();

export const DB_CONFIG = {
  user: "postgres",
  host: "127.0.0.1",
  database: "airbyte_ci_source",
  password: "secret_password",
  port: 5433,
};

export default defineConfig({
  projectId: "916nvw",
  viewportHeight: 800,
  viewportWidth: 1280,
  e2e: {
    baseUrl: "https://localhost:3000",
    specPattern: ["cypress/e2e/**/*.cy.ts"],
    supportFile: "cypress/support/e2e.ts",
    setupNodeEvents(on) {
      on("task", {
        dbQuery(params) {
          const { query, connection = DB_CONFIG } = params;
          const db = pgp(connection);
          return db.any(query).finally(db.$pool.end);
        },
      });
    },
  },
  retries: {
    runMode: 5,
    openMode: 0,
  },
  defaultCommandTimeout: 10000,
});
