import { assertOnLoginPage, testRedirectParamParsing } from "@cy/support/login-signup";
import { buildRegExp, UUID } from "support/regexp";
import { testUser } from "support/test-users";

describe("manually logging in and out of airbyte cloud", () => {
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
    cy.logout();
  });
  // TODO: fix flakiness before enabling again
  it.skip("passes a login redirect param between pages in login/signup flow if user attempts to access webapp before logging in", () => {
    // empty redirect url
    testRedirectParamParsing("/", true);
    // navigating directly to login page
    testRedirectParamParsing("/login");
    // redirect after logout
    testRedirectParamParsing("/settings/account");
    // non-empty redirect url with no search params
    testRedirectParamParsing("/workspaces/abcdefghi/source");
    // redirect url with search params
    testRedirectParamParsing("/workspaces/abcdefghi/connections/connections/new-connection?sourceId=1234567");
    // redirect url with search params and hash
    testRedirectParamParsing("/workspaces/abcdefghi/connections/123456789/job-history#37291::1");
  });
});
