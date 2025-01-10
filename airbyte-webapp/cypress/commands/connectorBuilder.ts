import {
  addStream,
  assertHasNumberOfPages,
  configureLimitOffsetPagination,
  configureParameters,
  enablePagination,
  enableParameterizedRequests,
  enterName,
  enterRecordSelector,
  enterStreamName,
  enterTestInputs,
  enterUrlBase,
  enterUrlPath,
  enterUrlPathFromForm,
  getDetectedSchemaElement,
  getSlicesFromDropdown,
  getUrlPathInput,
  goToTestPage,
  goToView,
  openDetectedSchemaTab,
  openStreamSchemaTab,
  openTestInputs,
  selectAuthMethod,
  submitForm,
} from "pages/connectorBuilderPage";

export const configureGlobals = (name: string) => {
  goToView("global");
  enterName(name);

  if (Cypress.env("MOCK_API_SERVER_HOST")) {
    enterUrlBase(Cypress.env("MOCK_API_SERVER_HOST"));
  } else if (Cypress.platform === "darwin") {
    enterUrlBase("http://host.docker.internal:6767/");
  } else {
    enterUrlBase("http://172.17.0.1:6767/");
  }

  configureAuth();
};

export const configureStream = () => {
  addStream();
  enterStreamName("Items");
  enterUrlPathFromForm("items/");
  submitForm();
  enterRecordSelector("items");
};

export const configureAuth = () => {
  goToView("global");
  selectAuthMethod("Bearer");
  openTestInputs();
  enterTestInputs({ apiKey: "theauthkey" });
  submitForm();
};

export const configurePagination = () => {
  goToView("0");
  enablePagination();
  configureLimitOffsetPagination("2", "Header", "limit", "Header", "offset");
};

export const configureParameterizedRequests = (numberOfParameters: number) => {
  goToView("0");
  enableParameterizedRequests();
  configureParameters(Array.from(Array(numberOfParameters).keys()).join(","), "item_id");
  enterUrlPath("items/{{}{{} stream_slice.item_id");
};

export const publishProject = () => {
  // debounce is 2500 so we need to wait at least more before change page
  // eslint-disable-next-line cypress/no-unnecessary-waiting
  cy.wait(3000);
  cy.get('[data-testid="publish-button"]').click();
  submitForm();
};

const testPanelContains = (str: string) => {
  cy.get("pre").contains(str).should("exist");
};

export const assertTestReadAuthFailure = () => {
  testPanelContains('"error": "Bad credentials"');
};

export const assertSource404Error = () => {
  testPanelContains('"status": 404');
};

export const assertTestReadItems = () => {
  testPanelContains('"name": "abc"');
  testPanelContains('"name": "def"');
};

export const assertMultiPageReadItems = () => {
  goToTestPage(1);
  assertTestReadItems();

  goToTestPage(2);
  testPanelContains('"name": "xxx"');
  testPanelContains('"name": "yyy"');

  goToTestPage(3);
  testPanelContains("[]");
};

const MAX_NUMBER_OF_PAGES = 5;
export const assertMaxNumberOfPages = () => {
  assertHasNumberOfPages(MAX_NUMBER_OF_PAGES);
};

export const assertHasNumberOfSlices = (numberOfSlices: number) => {
  getSlicesFromDropdown().should("have.length", numberOfSlices);
};

const MAX_NUMBER_OF_SLICES = 5;
export const assertMaxNumberOfSlices = () => {
  assertHasNumberOfSlices(MAX_NUMBER_OF_PAGES);
};

export const assertMaxNumberOfSlicesAndPages = () => {
  for (let i = 0; i < MAX_NUMBER_OF_SLICES; i++) {
    getSlicesFromDropdown()
      .contains(`Partition ${i + 1}`)
      .click();
    assertMaxNumberOfPages();
  }
};

const SCHEMA =
  " {\n" +
  '   "$schema": "http://json-schema.org/schema#",\n' +
  '   "additionalProperties": true,\n' +
  '   "properties": {\n' +
  '     "name": {\n' +
  '       "type": [\n         "string",\n         "null"\n       ]\n' +
  "     }\n" +
  "   },\n" +
  '   "type": "object"\n' +
  " }";
export const assertSchema = () => {
  openDetectedSchemaTab();
  getDetectedSchemaElement().should(($el) => {
    expect($el.get(0).innerText).to.eq(SCHEMA);
  });
};

const SCHEMA_WITH_MISMATCH =
  '{{}"$schema": "http://json-schema.org/schema#", "properties": {{}"name": {{}"type": "number"}}, "type": "object"}';
export const acceptSchemaWithMismatch = () => {
  openStreamSchemaTab();
  // When running against local dev webapp, some uncaught exceptions may be thrown when the monaco editor is loaded.
  // Ignore them since they do not affect the test.
  cy.on("uncaught:exception", (err) => {
    const monacoLoadCancelled = (err as { msg?: string })?.msg?.includes("operation is manually canceled");
    const monacoScriptLoadFailed = err?.message?.includes("importScripts") && err?.message?.includes("monaco-editor");
    if (monacoLoadCancelled || monacoScriptLoadFailed) {
      return false;
    }
  });
  cy.get("label").contains("Automatically import detected schema").click();
  cy.get("textarea").clear();
  cy.get("textarea").type(SCHEMA_WITH_MISMATCH);
};

export const assertSchemaMismatch = () => {
  openDetectedSchemaTab();
  cy.contains("Detected schema and declared schema are different").should("exist");
};

export const assertUrlPath = (urlPath: string) => {
  getUrlPathInput().contains(urlPath);
};

export const acceptSchema = () => {
  openDetectedSchemaTab();
  cy.get("[data-testid='accept-schema']").click();
};

export const focusAndType = (selector: string, text: string) => {
  cy.get(selector).click();
  cy.get(selector).type(text);
};
