import { buildRegExp, UUID } from "support/regexp";
import { testUser } from "support/test-users";

function assertOnLoginPage() {
  cy.hasNavigatedTo("/login");
  cy.contains("Log in to Airbyte");
}

describe("manually logging in and out of airbyte cloud", () => {
  it("can be done by entering credentials, navigating to a workspace's settings page, and clicking the sign out button", () => {
    cy.visit("/");
    // unauthenticated users are redirected to /login
    assertOnLoginPage();

    cy.get("[data-testid='input.email']").type(testUser.email);
    cy.get("[data-testid='login.password']").type(testUser.password);
    cy.get("[data-testid='login.submit']").click();
    cy.hasNavigatedTo("/workspaces");
    cy.selectWorkspace();

    cy.hasNavigatedTo(buildRegExp("/workspaces/", UUID, "/connections"));

    // Using `.click({ force: true })` can cause the button to be clicked while it's still
    // obscured by the loading indicator, which makes the test's DOM snapshots less
    // helpful; but it does prevent the need for awkward or brittle workarounds when the
    // cookies banner obscures the settings button.
    cy.contains("Settings").click({ force: true });

    cy.get("[data-testid='button.signout']").click({ force: true });

    // after logging out, users should again be redirected to the login page
    assertOnLoginPage();
  });
});
