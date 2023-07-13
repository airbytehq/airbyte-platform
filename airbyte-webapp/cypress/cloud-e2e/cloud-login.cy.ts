import { buildRegExp, UUID } from "support/regexp";
import { testUser } from "support/test-users";

function assertOnLoginPage() {
  cy.hasNavigatedTo("/login");
  cy.contains("Log in to Airbyte");
}

describe("manually logging in and out of airbyte cloud", () => {
  after(() => {
    cy.logout();
  });
  it("can be done by entering credentials, navigating to a workspace's settings page, and clicking the sign out button", () => {
    cy.visit("/");
    // unauthenticated users are redirected to /login
    assertOnLoginPage();

    cy.get("[data-testid='login.email']").type(testUser.email);
    cy.get("[data-testid='login.password']").type(testUser.password);
    cy.get("[data-testid='login.submit']").click();
    cy.hasNavigatedTo("/workspaces");
    cy.selectWorkspace();

    cy.hasNavigatedTo(buildRegExp("/workspaces/", UUID, "/connections"));
  });
});
