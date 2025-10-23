import { closeDbConnection } from "./helpers/database";

/**
 * Global teardown for Playwright tests
 * Ensures database connection pool is properly closed after all tests complete
 */
export default async function globalTeardown() {
  console.log("[Global Teardown] Closing database connection pool...");
  await closeDbConnection();
  console.log("[Global Teardown] Database connection pool closed successfully");
}
