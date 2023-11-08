export function assertOnLoginPage() {
  cy.hasNavigatedTo("/login");
  cy.contains("Log in to Airbyte");
}
const parseLoginRedirectParam = (search?: string) => {
  const params = new URLSearchParams(search);
  const loginRedirect = params.get("loginRedirect");
  return loginRedirect;
};

export const testRedirectParamParsing = (initialPath: string, wait?: boolean) => {
  const pathsWithNoRedirect = ["/", "/login", "/signup", "/settings/account"];
  const expectedRedirectPath = !pathsWithNoRedirect.includes(initialPath) ? initialPath : null;

  cy.visit(`${initialPath}`);
  // check login redirect parameter on login page
  assertOnLoginPage();
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);

  // wait for cookie banner to disappear
  // eslint-disable-next-line cypress/no-unnecessary-waiting
  wait && cy.wait(6000);

  // check login redirect parameter after navigating to sign up page
  cy.contains("Sign up").click();
  cy.hasNavigatedTo("/signup");
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);

  // check parameters on sign up with email page -- loginRedirect is unchanged and that method=email was set
  cy.contains("Sign up using email").click();
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);
  cy.location("search").then((search) => {
    const params = new URLSearchParams(search);
    const method = params.get("method");
    expect(method).to.equal("email");
  });

  // check that method=email was removed and loginRedirect was unchanged
  cy.contains("Sign up using Google or GitHub").click();
  cy.location("search").then((search) => {
    const params = new URLSearchParams(search);
    const method = params.get("method");
    expect(method).to.be.null;
  });
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);

  // check that loginRedirect is unchanged on password reset page
  cy.contains("Log in").click();
  cy.contains("Forgot your password").click();
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);

  // the Back to Log in link preserves the loginRedirect param as well
  cy.contains("Back to Log in").click();
  cy.location("search")
    .then((search) => parseLoginRedirectParam(search))
    .should("eq", expectedRedirectPath);
};
