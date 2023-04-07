const startFromScratchButton = "button[data-testid='start-from-scratch']";
const nameInput = "input[name='global.connectorName']";
const urlBaseInput = "input[name='global.urlBase']";
const addStreamButton = "button[data-testid='add-stream']";
const apiKeyInput = "input[name='connectionConfiguration.api_key']";
const toggleInput = "input[data-testid='toggle']";
const streamNameInput = "input[name='streamName']";
const streamUrlPathFromModal = "input[name='urlPath']";
const streamUrlPathFromForm = "input[name='streams[0].urlPath']";
const recordSelectorInput = "[data-testid='tag-input'] input";
const authType = "[data-testid='global.authenticator']";
const testInputsButton = "[data-testid='test-inputs']";
const limitInput = "[name='streams[0].paginator.strategy.page_size']";
const injectLimitInto = "[data-testid$='paginator.pageSizeOption.inject_into']";
const injectLimitFieldName = "[name='streams[0].paginator.pageSizeOption.field_name']";
const injectOffsetInto = "[data-testid$='paginator.pageTokenOption.inject_into']";
const injectOffsetFieldName = "[name='streams[0].paginator.pageTokenOption.field_name']";
const testPageItem = "[data-testid='test-pages'] li";
const submit = "button[type='submit']";
const testStreamButton = "button[data-testid='read-stream']";
const schemaDiff = 'pre[class*="SchemaDiffView"]';
const sliceDropdown = '[data-testid="tag-select-slice"]';

export const goToConnectorBuilderCreatePage = () => {
  cy.visit("/connector-builder/create");
  cy.wait(3000);
};

export const goToConnectorBuilderProjectsPage = () => {
  cy.visit("/connector-builder");
  cy.wait(3000);
};

export const editProjectBuilder = (name: string) => {
  cy.get(`button[data-testid='edit-project-button-${name}']`).click();
  cy.wait(3000);
};

export const startFromScratch = () => {
  cy.get(startFromScratchButton).click({ force: true });
};

export const enterName = (name: string) => {
  cy.get(nameInput).clear().type(name, { force: true });
};

export const enterUrlBase = (urlBase: string) => {
  cy.get(urlBaseInput).type(urlBase, { force: true });
};

export const enterRecordSelector = (recordSelector: string) => {
  cy.get(recordSelectorInput).first().type(recordSelector, { force: true }).type("{enter}", { force: true });
};

const selectFromDropdown = (selector: string, value: string) => {
  cy.get(`${selector} .react-select__dropdown-indicator`).last().click({ force: true });

  cy.get(`.react-select__option`).contains(value).click();
};

export const selectAuthMethod = (value: string) => {
  selectFromDropdown(authType, value);
};

export const selectActiveVersion = (name: string, version: number) => {
  cy.get(`[data-testid='version-changer-${name}']`).click();
  cy.get("[data-testid='versions-list'] > button").contains(`v${version}`).click();
};

export const goToView = (view: string) => {
  cy.get(`button[data-testid=navbutton-${view}]`).click();
};

export const openTestInputs = () => {
  cy.get(testInputsButton).click();
};

export const enterTestInputs = ({ apiKey }: { apiKey: string }) => {
  cy.get(apiKeyInput).type(apiKey, { force: true });
};

export const goToTestPage = (page: number) => {
  cy.get(testPageItem).contains(page).click();
};

const getPaginationCheckbox = () => {
  return cy.get(toggleInput).first();
};

export const enablePagination = () => {
  getPaginationCheckbox().check({ force: true });
};

export const disablePagination = () => {
  getPaginationCheckbox().uncheck({ force: true });
};

export const configureLimitOffsetPagination = (
  limit: string,
  limitInto: string,
  limitFieldName: string,
  offsetInto: string,
  offsetFieldName: string
) => {
  cy.get(limitInput).type(limit, { force: true });
  selectFromDropdown(injectLimitInto, limitInto);
  cy.get(injectLimitFieldName).type(limitFieldName);
  selectFromDropdown(injectOffsetInto, offsetInto);
  cy.get(injectOffsetFieldName).type(offsetFieldName, { force: true });
};

const getStreamSlicerCheckbox = () => {
  return cy.get(toggleInput).eq(2);
};

export const enableStreamSlicer = () => {
  getStreamSlicerCheckbox().check({ force: true });
};

export const disableStreamSlicer = () => {
  getStreamSlicerCheckbox().uncheck({ force: true });
};

export const configureListStreamSlicer = (values: string, cursor_field: string) => {
  cy.get('[data-testid="tag-input-streams[0].partitionRouter[0].values"] input[type="text"]').type(values, {
    force: true,
  });
  cy.get("[name='streams[0].partitionRouter[0].cursor_field']").type(cursor_field, { force: true });
};

export const getSlicesFromDropdown = () => {
  cy.get(`${sliceDropdown} button`).click();
  return cy.get(`${sliceDropdown} li`);
};

export const openStreamSchemaTab = () => {
  cy.get('[data-testid="tag-tab-stream-schema"]').click();
};

export const openDetectedSchemaTab = () => {
  cy.get('[data-testid="tag-tab-detected-schema"]').click();
};

export const getDetectedSchemaElement = () => {
  return cy.get('pre[class*="SchemaDiffView"]');
};

export const addStream = () => {
  cy.get(addStreamButton).click();
};

export const enterStreamName = (streamName: string) => {
  cy.get(streamNameInput).type(streamName, { force: true });
};

export const enterUrlPathFromForm = (urlPath: string) => {
  cy.get(streamUrlPathFromModal).type(urlPath, { force: true });
};

export const getUrlPathInput = () => {
  return cy.get(streamUrlPathFromForm);
};

export const enterUrlPath = (urlPath: string) => {
  cy.get('[name="streams[0].urlPath"]').focus().clear().type(urlPath, { force: true });
};

export const submitForm = () => {
  cy.get(submit).click();
};

export const testStream = () => {
  // wait for debounced form
  cy.wait(500);
  cy.get(testStreamButton).click();
};

const GO_BACK_AND_GO_NEXT_BUTTONS = 2;
export const assertHasNumberOfPages = (numberOfPages: number) => {
  for (let i = 0; i < numberOfPages; i++) {
    cy.get(testPageItem)
      .contains(i + 1)
      .should("exist");
  }

  cy.get(testPageItem).should("have.length", numberOfPages + GO_BACK_AND_GO_NEXT_BUTTONS);
};
