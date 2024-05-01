import { appendRandomString } from "@cy/commands/common";

const builderLink = "a[data-testid='builderLink']";
const nameInput = "input[name='name']";

describe("running the connector builder on cloud", () => {
  let connectorName = "";
  beforeEach(() => {
    connectorName = appendRandomString("localhost");
    cy.login();
    cy.selectWorkspace();
    cy.get(builderLink, { timeout: Cypress.config("pageLoadTimeout") }).click();

    // handle case where the workspace already has a builder project
    cy.url().then((url) => {
      if (url.endsWith("/connector-builder")) {
        cy.visit(`${url}/create`);
      }
    });
  });

  afterEach(() => {
    cy.get(builderLink, { timeout: Cypress.config("pageLoadTimeout") }).click();
    cy.get(`button[data-testid='delete-project-button_${connectorName}']`).click();
    cy.contains("Delete").click();
    cy.logout();
  });

  // try to run a test read against localhost to verify that the builder can execute the CDK
  // without needing to rely on an actual API or set up a mock server
  it("returns an error when trying to run against localhost", () => {
    cy.get("button[data-testid='start-from-scratch']", { timeout: Cypress.config("pageLoadTimeout") }).click();
    cy.get(nameInput, { timeout: Cypress.config("pageLoadTimeout") }).clear();
    cy.get(nameInput).type(connectorName);
    cy.get("input[name='formValues.global.urlBase']").type("https://localhost:8000");
    cy.get("button[data-testid='add-stream']").click();
    cy.get("input[name='streamName']").type("test_stream");
    cy.get("input[name='urlPath']").type(`test_path{enter}`);
    cy.get("button[data-testid='read-stream']").click();
    cy.get("pre", { timeout: 30000 }).contains(
      "Invalid URL endpoint: The endpoint that data is being requested from belongs to a private network."
    );
  });
});
