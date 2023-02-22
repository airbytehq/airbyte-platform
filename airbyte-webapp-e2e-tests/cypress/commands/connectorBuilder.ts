import {
  addStream,
  assertHasNumberOfPages,
  configureListStreamSlicer,
  configureOffsetPagination,
  disablePagination,
  disableStreamSlicer,
  enableStreamSlicer,
  enablePagination,
  enterName,
  enterRecordSelector,
  enterStreamName,
  enterTestInputs,
  enterUrlBase,
  enterUrlPath,
  enterUrlPathFromForm,
  goToTestPage,
  goToView,
  openTestInputs,
  selectAuthMethod,
  submitForm,
} from "pages/connectorBuilderPage";

export const configureGlobals = () => {
  goToView("global");
  enterName("Dummy API");
  enterUrlBase("http://dummy_api:6767/");
}

export const configureStream = () => {
  addStream();
  enterStreamName("Items");
  enterUrlPathFromForm("items/");
  submitForm();
  enterRecordSelector("items");
}

export const configureAuth = () => {
  goToView("global");
  selectAuthMethod("Bearer");
  openTestInputs();
  enterTestInputs({ apiKey: "theauthkey" })
  submitForm();
  goToView("0");
}

export const configurePagination = () => {
  goToView("0");
  enablePagination();
  configureOffsetPagination("2", "header", "offset");
}

export const configureStreamSlicer = (numberOfSlices: number) => {
  goToView("0");
  enableStreamSlicer();
  configureListStreamSlicer(Array.from(Array(numberOfSlices).keys()).join(","), "item_id");
  enterUrlPath("items/{{}{{} stream_slice.item_id }}");
}

export const cleanUp = () => {
  goToView("0");
  cy.get('[data-testid="tag-tab-stream-configuration"]').click({ force: true });
  disablePagination();
  disableStreamSlicer();
}

const testPanelContains = (str: string) => {
  cy.get("pre").contains(str).should("exist");
}

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
  testPanelContains('[]');
};

const MAX_NUMBER_OF_PAGES = 5;
export const assertMaxNumberOfPages = () => {
  assertHasNumberOfPages(MAX_NUMBER_OF_PAGES)
};

export const assertHasNumberOfSlices = (numberOfSlices: number) => {
  cy.get('[data-testid="tag-select-slice"] button').click();
  cy.get('[data-testid="tag-select-slice"] li').should('have.length', numberOfSlices);
};

const MAX_NUMBER_OF_SLICES = 5;
export const assertMaxNumberOfSlices = () => {
  assertHasNumberOfSlices(MAX_NUMBER_OF_PAGES)
};

export const assertMaxNumberOfSlicesAndPages = () => {
  for (var i = 0; i < MAX_NUMBER_OF_SLICES; i++) {
    cy.get('[data-testid="tag-select-slice"] button').click();
    cy.get('[data-testid="tag-select-slice"] li').contains("Slice " + i).click();
    assertMaxNumberOfPages();
  }
};

const SCHEMA =  ' {\n' +
'   "$schema": "http://json-schema.org/schema#",\n' +
'   "properties": {\n' +
'     "name": {\n' +
'       "type": "string"\n' +
'     }\n' +
'   },\n' +
'   "type": "object"\n' +
' }'
export const assertSchema = () => {
  cy.get('[data-testid="tag-tab-detected-schema"]').click();
  cy.get('pre[class*="SchemaDiffView"]').contains(SCHEMA).should("exist");
};

const SCHEMA_WITH_MISMATCH = '{{}"$schema": "http://json-schema.org/schema#", "properties": {{}"name": {{}"type": "number"}}, "type": "object"}'
export const acceptSchemaWithMismatch = () => {
  cy.get('[data-testid="tag-tab-stream-schema"]').click();
  cy.get('textarea').type(SCHEMA_WITH_MISMATCH, { force: true });
};

export const assertSchemaMismatch = () => {
  cy.get('[data-testid="tag-tab-detected-schema"]').click();
  cy.contains("Detected schema and declared schema are different").should("exist");
  cy.get('pre[class*="SchemaDiffView"]').contains('-      "type": "number"').should("exist");
  cy.get('pre[class*="SchemaDiffView"]').contains('+      "type": "string"').should("exist");
};
