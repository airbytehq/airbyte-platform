import { appendRandomString } from "@cy/commands/common";
import { assertOnLoginPage } from "@cy/support/login-signup";
import { buildRegExp, UUID } from "support/regexp";

describe("registering for airbyte cloud", () => {
  it("can be done by entering an email and password", () => {
    cy.visit("/");
    assertOnLoginPage();

    cy.get("button:contains('Sign up')").click();
    cy.get("button:contains('Sign up using email')").click();

    cy.get("[data-testid='signup.companyName']").type("E2E Inc");
    cy.get("[data-testid='signup.email']").type(`${appendRandomString("e2e-user")}@airbyte.io`);
    cy.get("[data-testid='password-input']").type(`${appendRandomString("Password")}`);
    cy.get("[data-testid='signup.submit']").click();

    // navigates directly to a newly created workspace
    cy.hasNavigatedTo(buildRegExp("/workspaces/", UUID, "/connections"));
    cy.logout();
  });
});
