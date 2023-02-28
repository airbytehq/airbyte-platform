import { goToConnectorBuilderPage, startFromScratch, testStream } from "pages/connectorBuilderPage";
import {
  assertTestReadItems,
  assertTestReadAuthFailure,
  configureAuth,
  configureGlobals,
  configureStream,
  configurePagination,
  assertMultiPageReadItems,
} from "commands/connectorBuilder";
import { initialSetupCompleted } from "commands/workspaces";

describe("Connector builder", { testIsolation: false }, () => {
  before(() => {
    // Updated for cypress 12 because connector builder uses local storage
    // docs.cypress.io/guides/references/migration-guide#Simulating-Pre-Test-Isolation-Behavior
    cy.clearLocalStorage();
    cy.clearCookies();

    initialSetupCompleted();
    goToConnectorBuilderPage();
    startFromScratch();
  });

  it("Configure basic connector", () => {
    configureGlobals();
    configureStream();
  });

  it("Fail on missing auth", () => {
    testStream();
    assertTestReadAuthFailure();
  });

  it("Succeed on provided auth", () => {
    configureAuth();
    testStream();
    assertTestReadItems();
  });

  it("Pagination", () => {
    configurePagination();
    testStream();
    assertMultiPageReadItems();
  });
});
