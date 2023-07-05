export interface TestUserCredentials {
  email: string;
  password: string;
}

export const testUser: TestUserCredentials = {
  email: Cypress.env("TEST_USER_EMAIL") || "integration-test@airbyte.io",
  password: Cypress.env("TEST_USER_PW"),
};
