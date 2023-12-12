import { getTestId } from "utils/selectors";

const connectionNameInput = getTestId("connectionName");
const expandConfigurationIcon = getTestId("configuration-card-expand-arrow");
export const scheduleTypeDropdown = getTestId("schedule-type-listbox-button");
export const basicScheduleDataDropdown = getTestId("basic-schedule-listbox-button");
export const getListBoxDropdownOption = (option: string) => `${getTestId(`${option.toLowerCase()}-option`)}`;

export const destinationPrefixEditButton = getTestId("destination-stream-prefix-edit-button");
const destinationPrefixApplyButton = getTestId("destination-stream-names-apply-button");
// const destinationPrefixCancelButton = getTestId("destination-stream-names-cancel-button");
// const destinationMirror = getTestId("destination-stream-names-mirror-radio");
const destinationPrefix = getTestId("destination-stream-names-prefix-radio");
const destinationPrefixInput = getTestId("destination-stream-names-prefix-input");

export const destinationNamespaceEditButton = getTestId("destination-namespace-edit-button");
// const destinationNamespaceCancelButton = getTestId("namespace-definition-cancel-button");
const destinationNamespaceApplyButton = getTestId("namespace-definition-apply-button");
const destinationNamespaceCustom = getTestId("namespace-definition-custom-format-radio");
const destinationNamespaceDestination = getTestId("namespace-definition-destination-radio");
const destinationNamespaceSource = getTestId("namespace-definition-source-radio");
const destinationNamespaceCustomInput = getTestId("namespace-definition-custom-format-input");

export const enterConnectionName = (name: string) => {
  cy.get(connectionNameInput).type(name);
};
/**
 * Select schedule type from dropdown: Schedule, Manual, Cron
 * @param value
 */
export const selectScheduleType = (value: "Scheduled" | "Manual" | "Cron") => {
  cy.get(scheduleTypeDropdown).click();
  cy.get(getListBoxDropdownOption(value)).click();
};
/**
 * Select schedule data from dropdown: Hourly
 * @param value
 */
export const selectBasicScheduleData = (value: string) => {
  cy.get(basicScheduleDataDropdown).click();
  // example: frequency-1-hours-option
  cy.get(getTestId(`frequency-${value.toLowerCase()}-option`)).click();
};

export const expandConfigurationSection = () => {
  cy.get(expandConfigurationIcon).click();
};

export const fillOutDestinationPrefix = (value: string) => {
  cy.get(destinationPrefixEditButton).click();
  cy.get(destinationPrefix).click({ force: true });
  cy.get(destinationPrefixInput).clear();
  cy.get(destinationPrefixInput).type(value);
  cy.get(destinationPrefixInput).should("have.value", value);
  cy.get(destinationPrefixApplyButton).click();
};

export const setupDestinationNamespaceCustomFormat = (value: string) => {
  cy.get(destinationNamespaceEditButton).click();
  cy.get(destinationNamespaceCustom).click({ force: true });
  cy.get(destinationNamespaceCustomInput).first().type(value);
  cy.get(destinationNamespaceCustomInput).first().should("have.value", `\${SOURCE_NAMESPACE}${value}`);
  cy.get(destinationNamespaceApplyButton).click();
};

export const setupDestinationNamespaceSourceFormat = () => {
  cy.get(destinationNamespaceEditButton).click();
  cy.get(destinationNamespaceSource).click({ force: true });
  cy.get(destinationNamespaceApplyButton).click();
};

export const setupDestinationNamespaceDefaultFormat = () => {
  cy.get(destinationNamespaceEditButton).click();
  cy.get(destinationNamespaceDestination).click({ force: true });
  cy.get(destinationNamespaceApplyButton).click();
};
