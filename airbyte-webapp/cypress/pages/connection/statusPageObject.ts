import { getTestId } from "utils/selectors";

export const connectionStatusIndicator = getTestId("connection-status-indicator");
export const manualSyncButton = getTestId("manual-sync-button");
export const jobHistoryDropdownMenu = getTestId("job-history-dropdown-menu");
export const resetDataDropdownOption = getTestId("reset-data-dropdown-option");

export const connectionStatusShouldBe = (status: string) => {
  cy.get(connectionStatusIndicator).invoke("attr", "data-status").should("eq", status);
};
