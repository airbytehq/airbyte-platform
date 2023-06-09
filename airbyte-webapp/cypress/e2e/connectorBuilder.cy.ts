import { appendRandomString } from "commands/common";
import { createTestConnection, startManualSync } from "commands/connection";
import {
  acceptSchemaWithMismatch,
  assertMaxNumberOfPages,
  assertMaxNumberOfSlices,
  assertMaxNumberOfSlicesAndPages,
  assertMultiPageReadItems,
  assertHasNumberOfSlices,
  assertTestReadItems,
  assertTestReadAuthFailure,
  assertSchema,
  assertSchemaMismatch,
  assertSource404Error,
  assertUrlPath,
  cleanUp,
  configureAuth,
  configureGlobals,
  configurePagination,
  configureStream,
  configureStreamSlicer,
  publishProject,
} from "commands/connectorBuilder";

import * as connectionSettings from "pages/connection/connectionSettingsPageObject";
import {
  editProjectBuilder,
  enterUrlPath,
  goToConnectorBuilderCreatePage,
  goToConnectorBuilderProjectsPage,
  goToView,
  selectActiveVersion,
  startFromScratch,
  testStream,
} from "pages/connectorBuilderPage";
import { goToSourcePage, openSourceOverview } from "pages/sourcePage";

describe("Connector builder", { testIsolation: false }, () => {
  const connectorName = appendRandomString("dummy_api");
  before(() => {
    // Updated for cypress 12 because connector builder uses local storage
    // docs.cypress.io/guides/references/migration-guide#Simulating-Pre-Test-Isolation-Behavior
    cy.clearLocalStorage();
    cy.clearCookies();

    goToConnectorBuilderCreatePage();
    startFromScratch();
    configureGlobals(connectorName);
    configureStream();
  });

  afterEach(() => {
    cleanUp();
  });

  /*
  This test assumes it runs before "Read - Without pagination or partition router" since auth will be configured at that
  point
  */
  it("Fail on invalid auth", () => {
    testStream();
    assertTestReadAuthFailure();
  });

  it("Read - Without pagination or partition router", () => {
    configureAuth();
    testStream();
    assertTestReadItems();
  });

  /*
  All the tests below assume they run after "Read - Without pagination or partition router" in order to have auth
  configured
  */
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
    enterUrlPath("items/");

    testStream();

    assertMultiPageReadItems();
  });

  it("Read - Pagination exceeding page limit", () => {
    configurePagination();
    enterUrlPath("items/exceeding-page-limit/");

    testStream();

    assertMaxNumberOfPages();
  });

  it("Read - With partition router", () => {
    configureStreamSlicer(3);
    testStream();
    assertHasNumberOfSlices(3);
  });

  it("Read - With partition router exceeding number of partitions", () => {
    configureStreamSlicer(10);
    testStream();
    assertMaxNumberOfSlices();
  });

  it("Read - Pagination & partition router exceeding limits", () => {
    configurePagination();
    configureStreamSlicer(10);

    testStream();

    assertMaxNumberOfSlicesAndPages();
  });

  // Note: This test cannot be run in isolation!  It is dependent on the previous test
  it("Sync published version", () => {
    publishProject();

    const sourceName = connectorName;
    const destinationName = appendRandomString("builder dest");
    createTestConnection(sourceName, destinationName);
    startManualSync();

    cy.get("span").contains("2 committed records", { timeout: 60000 }).should("exist");

    // release new connector version
    goToConnectorBuilderProjectsPage();
    editProjectBuilder(connectorName);
    configurePagination();
    publishProject();
    sync(sourceName, destinationName);

    cy.get("span").contains("4 committed records", { timeout: 60000 }).should("exist");

    goToConnectorBuilderProjectsPage();
    selectActiveVersion(connectorName, 1);
    sync(sourceName, destinationName);

    cy.get('span:contains("2 committed records")', { timeout: 60000 }).should("have.length", 2);

    goToConnectorBuilderProjectsPage();
    editProjectBuilder(connectorName);
  });

  // This test assumes the test before is configuring path items/
  it("Validate going back to a previously created connector", () => {
    goToConnectorBuilderProjectsPage();
    editProjectBuilder(connectorName);
    goToView("0");
    assertUrlPath("items/{{ stream_slice.item_id }}");
  });
});

const sync = (sourceName: string, destinationName: string) => {
  goToSourcePage();
  openSourceOverview(sourceName);
  connectionSettings.openConnectionOverviewByDestinationName(destinationName);
  startManualSync();
};
