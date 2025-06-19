import { v4 as uuidv4 } from "uuid";

/**
 * ensures a unique email is generated for each test run
 *
 * this is useful in case you need to run the tests multiple times locally on a given deployment
 */

const uniqueId = uuidv4();
export const testEmail = `integration-test+${uniqueId}@airbyte.com`;
export const testPassword = "passwordpassword";
