import { appendRandomString } from "commands/common";
import {
  acceptSchemaWithMismatch,
  assertHasNumberOfSlices,
  assertMaxNumberOfPages,
  assertMaxNumberOfSlices,
  assertMaxNumberOfSlicesAndPages,
  assertMultiPageReadItems,
  assertSchema,
  assertSchemaMismatch,
  assertSource404Error,
  assertTestReadAuthFailure,
  assertTestReadItems,
  assertUrlPath,
  configureGlobals,
  configurePagination,
  configureParameterizedRequests,
  configureStream,
  publishProject,
} from "commands/connectorBuilder";

import {
  editProjectBuilder,
  enterUrlPath,
  goToConnectorBuilderCreatePage,
  goToConnectorBuilderProjectsPage,
  goToView,
  selectAuthMethod,
  startFromScratch,
  testStream,
} from "pages/connectorBuilderPage";

describe("Connector builder", { testIsolation: false, tags: "@builder" }, () => {
  let connectorName = "";
  beforeEach(() => {
    cy.on("uncaught:exception", () => false);
    connectorName = appendRandomString("dummy_api");
    // Updated for cypress 12 because connector builder uses local storage
    // docs.cypress.io/guides/references/migration-guide#Simulating-Pre-Test-Isolation-Behavior
    cy.clearLocalStorage();
    cy.clearCookies();

    goToConnectorBuilderCreatePage();
    startFromScratch();
    configureGlobals(connectorName);
    configureStream();
  });

  it("Fail on invalid auth", () => {
    cy.on("uncaught:exception", () => false);
    goToView("global");
    selectAuthMethod("No Auth");
    testStream();
    assertTestReadAuthFailure();
  });

  it("Read - Without pagination or parameterized requests", () => {
    testStream();
    assertTestReadItems();
  });

  it("Read - Infer schema", () => {
    testStream();
    assertSchema();
  });

  it("Read - Schema mismatch", () => {
    acceptSchemaWithMismatch();
    testStream();
    assertSchemaMismatch();
  });

  it("Read - Read with 404 error", () => {
    enterUrlPath("resource-not-found/");
    testStream();
    assertSource404Error();
  });

  it("Read - With pagination", () => {
    configurePagination();
    testStream();
    assertMultiPageReadItems();
  });

  it("Read - Pagination exceeding page limit", () => {
    configurePagination();
    enterUrlPath("items/exceeding-page-limit/");

    testStream();

    assertMaxNumberOfPages();
  });

  it("Read - With parameterized requests", () => {
    configureParameterizedRequests(3);
    testStream();
    assertHasNumberOfSlices(3);
  });

  it("Read - With parameterized requests exceeding number of slices", () => {
    configureParameterizedRequests(10);
    testStream();
    assertMaxNumberOfSlices();
  });

  it("Read - Pagination & parameterized requests exceeding limits", () => {
    configurePagination();
    configureParameterizedRequests(10);

    testStream();

    assertMaxNumberOfSlicesAndPages();
  });

  // Commented out because it's pretty flaky, which slows down the test suite.
  // it("Sync published version", () => {
  //   testStream();
  //   publishProject();
  //
  //   const sourceName = connectorName;
  //   const destinationName = appendRandomString("builder dest");
  //   createTestConnection(sourceName, destinationName);
  //   startManualSync();
  //
  //   cy.get("[data-testid='job-history-step']").click();
  //   cy.get("span").contains("2 records loaded", { timeout: 60000 }).should("exist");
  //
  //   // release new connector version
  //   goToConnectorBuilderProjectsPage();
  //   editProjectBuilder(connectorName);
  //   configurePagination();
  //   testStream();
  //   publishProject();
  //   sync(sourceName, destinationName);
  //
  //   cy.get("[data-testid='job-history-step']").click();
  //   cy.get("span").contains("4 records loaded", { timeout: 60000 }).should("exist");
  //
  //   goToConnectorBuilderProjectsPage();
  //   selectActiveVersion(connectorName, 1);
  //   sync(sourceName, destinationName);
  //
  //   cy.get("[data-testid='job-history-step']").click();
  //   cy.get('span:contains("2 records loaded")', { timeout: 60000 }).should("have.length", 2);
  //
  //   goToConnectorBuilderProjectsPage();
  //   editProjectBuilder(connectorName);
  // });

  it("Validate going back to a previously created connector", () => {
    configureParameterizedRequests(10);
    testStream();
    publishProject();
    goToConnectorBuilderProjectsPage();
    editProjectBuilder(connectorName);
    goToView("0");
    assertUrlPath("items/{{ stream_slice.item_id }}");
  });
});

// const sync = (sourceName: string, destinationName: string) => {
//   goToSourcePage();
//   openSourceConnectionsPage(sourceName);
//   connectionSettings.openConnectionOverviewByDestinationName(destinationName);
//   startManualSync();
// };
