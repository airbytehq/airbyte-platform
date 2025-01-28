import { selectFromDropdown } from "@cy/commands/common";
import { focusAndType } from "@cy/commands/connectorBuilder";

const startFromScratchButton = "[data-testid='start-from-scratch']";
const nameLabel = "[data-testid='connector-name-label']";
const nameInput = "[data-testid='connector-name-input']";
const urlBaseInput = "[name='formValues.global.urlBase']";
const addStreamButton = "[data-testid='add-stream']";
const apiKeyInput = "input[name='connectionConfiguration.api_key']";
const togglePaginationInput = "[data-testid='toggle-formValues.streams.0.paginator']";
const toggleParameterizedRequestsInput = "input[data-testid='toggle-formValues.streams.0.parameterizedRequests']";
const parameterizedRequestsCursorInput = "[name='formValues.streams.0.parameterizedRequests.0.cursor_field']";
const streamNameInput = "input[name='streamName']";
const streamUrlPathFromModal = "[name='urlPath']";
const streamUrlPathFromForm = "[name='formValues.streams.0.urlPath']";
const recordSelectorToggle = "[data-testid='toggle-formValues.streams.0.recordSelector']";
const recordSelectorFieldPathInput = "[data-testid='tag-input-formValues.streams.0.recordSelector.fieldPath'] input";
const authType = "[data-testid='formValues.global.authenticator.type']";
const testInputsButton = "[data-testid='test-inputs']";
const limitInput = "[name='formValues.streams.0.paginator.strategy.page_size']";
const injectLimitInto = "[data-testid$='paginator.pageSizeOption.inject_into']";
const injectLimitFieldName = "[name='formValues.streams.0.paginator.pageSizeOption.field_name']";
const injectOffsetInto = "[data-testid$='paginator.pageTokenOption.inject_into']";
const injectOffsetFieldName = "[name='formValues.streams.0.paginator.pageTokenOption.field_name']";
const testPageItem = "[data-testid='test-pages'] li";
const submit = "button[type='submit']";
const testStreamButton = "[data-testid='read-stream']";
const sliceDropdown = '[data-testid="tag-select-slice"]';

export const goToConnectorBuilderCreatePage = () => {
  cy.visit("/connector-builder/create");
};

export const goToConnectorBuilderProjectsPage = () => {
  cy.visit("/connector-builder");
};

export const editProjectBuilder = (name: string) => {
  cy.get(`[data-testid='edit-project-button-${name}']`).click();
};

export const startFromScratch = () => {
  cy.get(startFromScratchButton, { timeout: 20000 }).click();
};

export const enterName = (name: string) => {
  cy.get(nameLabel).first().click();
  cy.get(nameInput).clear();
  cy.get(nameInput).type(name);
};

export const enterUrlBase = (urlBase: string) => {
  focusAndType(urlBaseInput, urlBase);
};

export const enterRecordSelector = (recordSelector: string) => {
  cy.get(recordSelectorToggle).parent().click();
  cy.get(recordSelectorFieldPathInput).first().type(recordSelector);
  cy.get(recordSelectorFieldPathInput).first().type("{enter}");
};

export const selectAuthMethod = (value: string) => {
  selectFromDropdown(authType, value);
};

export const selectActiveVersion = (name: string, version: number) => {
  cy.get(`[data-testid='version-changer-${name}']`).click();
  cy.get("[data-testid='versions-list'] > button").contains(`v${version}`).click();
};

export const goToView = (view: string) => {
  cy.get(`button[data-testid=navbutton-${view}]`, { timeout: 20000 }).click();
};

export const openTestInputs = () => {
  cy.get(testInputsButton).click();
};

export const enterTestInputs = ({ apiKey }: { apiKey: string }) => {
  cy.get(apiKeyInput).type(apiKey);
};

export const goToTestPage = (page: number) => {
  cy.get(testPageItem).contains(page).click();
};

export const enablePagination = () => {
  // force: true is needed because the input has display: none, as we don't want to show default checkboxes
  cy.get(togglePaginationInput).check({ force: true });
};

export const configureLimitOffsetPagination = (
  limit: string,
  limitInto: string,
  limitFieldName: string,
  offsetInto: string,
  offsetFieldName: string
) => {
  cy.get(limitInput).type(limit);
  selectFromDropdown(injectLimitInto, limitInto);
  focusAndType(injectLimitFieldName, limitFieldName);
  selectFromDropdown(injectOffsetInto, offsetInto);
  focusAndType(injectOffsetFieldName, offsetFieldName);
};

export const enableParameterizedRequests = () => {
  // force: true is needed because the input has display: none, as we don't want to show default checkboxes
  cy.get(toggleParameterizedRequestsInput).check({ force: true });
};

export const configureParameters = (values: string, cursor_field: string) => {
  cy.get('[data-testid="tag-input-formValues.streams.0.parameterizedRequests.0.values.value"] input[type="text"]').type(
    values
  );
  focusAndType(parameterizedRequestsCursorInput, cursor_field);
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
  cy.get(streamNameInput).type(streamName);
};

export const enterUrlPathFromForm = (urlPath: string) => {
  focusAndType(streamUrlPathFromModal, urlPath);
};

export const getUrlPathInput = () => {
  return cy.get(streamUrlPathFromForm);
};

export const enterUrlPath = (urlPath: string) => {
  focusAndType(streamUrlPathFromForm, "{selectAll}{backspace}");
  cy.get(streamUrlPathFromForm).type(urlPath);
};

export const submitForm = () => {
  cy.get(submit).click();
};

export const testStream = () => {
  // wait for debounced form
  // eslint-disable-next-line cypress/no-unnecessary-waiting
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
