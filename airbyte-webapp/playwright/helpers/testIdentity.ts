import { v4 as uuidv4 } from "uuid";

/**
 * ensures a unique email is generated for each test run
 *
 * this is useful in case you need to run the tests multiple times locally on a given deployment
 */

const uniqueId = uuidv4();
export const testEmail = `integration-test+${uniqueId}@airbyte.com`;
// Derived from a random UUID so it stays out of the Keycloak password blacklist
// while satisfying the realm's length policy.
export const testPassword = `test-${uniqueId}`;
