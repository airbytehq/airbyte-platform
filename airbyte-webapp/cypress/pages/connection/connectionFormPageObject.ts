import { getTestId } from "utils/selectors";

const connectionNameInput = getTestId("connectionName");
export const scheduleTypeDropdown = getTestId("schedule-type-listbox-button");
export const basicScheduleDataDropdown = getTestId("basic-schedule-listbox-button");
export const getListBoxDropdownOption = (option: string) => `${getTestId(`${option.toLowerCase()}-option`)}`;

export const destinationNamespaceListBox = `${getTestId("namespace-definition-listbox-button")}`;
export const destinationNamespaceListBoxOptionCustom = getTestId("custom-option");
export const destinationNamespaceListBoxOptionSource = getTestId("source-option");
export const destinationNamespaceListBoxOptionDestination = getTestId("destination-option");
const destinationNamespaceCustomInput = getTestId("namespace-definition-custom-format-input");
export const destinationNamespaceCustomPreview = getTestId("custom-namespace-preview");
export const destinationNamespaceSourcePreview = getTestId("source-namespace-preview");

export const streamPrefixInput = getTestId("stream-prefix-input");
export const streamPrefixPreview = getTestId("stream-prefix-preview");

export const nonBreakingChangesPreference = "[data-testid='nonBreakingChangesPreference-listbox-button']";
const nonBreakingChangesPreferenceValue = (value: string) => `[data-testid='${value}-option']`;

const advancedSettingsButton = getTestId("advanced-settings-button");

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

export const setupDestinationNamespaceCustomFormat = (value: string) => {
  cy.get(destinationNamespaceListBox).click();
  cy.get(destinationNamespaceListBoxOptionCustom).click();
  cy.get(destinationNamespaceCustomInput).first().type(value);
  cy.get(destinationNamespaceCustomInput).first().should("have.value", `\${SOURCE_NAMESPACE}${value}`);
};

export const setupDestinationNamespaceSourceFormat = (isCreating = false) => {
  if (!isCreating) {
    cy.get(destinationNamespaceListBox).click();
  }
  cy.get(destinationNamespaceListBoxOptionSource).click({ force: true });
};

export const setupDestinationNamespaceDestinationFormat = () => {
  cy.get(destinationNamespaceListBox).click();
  cy.get(destinationNamespaceListBoxOptionDestination).click({ force: true });
};

export const toggleAdvancedSettingsSection = () => {
  cy.get(advancedSettingsButton).click();
};

export const selectNonBreakingChangesPreference = (preference: "ignore" | "disable") => {
  cy.get(nonBreakingChangesPreference).click();
  cy.get(nonBreakingChangesPreferenceValue(preference)).click({ force: true });
};
