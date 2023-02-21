import { goToConnectorBuilderPage, startFromScratch, enterUrlPath, testStream } from "pages/connectorBuilderPage";
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
  cleanUp,
  configureAuth,
  configureGlobals,
  configurePagination,
  configureStream,
  configureStreamSlicer,
  invalidAuth,
} from "commands/connectorBuilder";

import { initialSetupCompleted } from "commands/workspaces";

describe("Connector builder", () => {
  before(() => {
    initialSetupCompleted();
    goToConnectorBuilderPage();
    startFromScratch();
    configureGlobals();
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
    enterUrlPath("items/exceeding-page-limit");

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

});
