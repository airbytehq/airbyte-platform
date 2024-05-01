import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { clickOnCellInTable, submitButtonClick } from "commands/common";
import { interceptUpdateConnectionRequest, waitForUpdateConnectionRequest } from "commands/interceptors";
import { RouteHandler } from "cypress/types/net-stubbing";

const settingsTab = "a[data-testid='settings-step']";
const sourceColumnName = "Source name";
const destinationColumnName = "Destination name";
const connectionsTable = "table[data-testid='connectionsTable']";
export const resetDataButton = "[data-testid='resetDataButton']";
export const deleteConnectionButton = "[data-testid='open-delete-modal']";

export const openConnectionOverviewBySourceName = (sourceName: string) => {
  clickOnCellInTable(connectionsTable, sourceColumnName, sourceName);
};

export const openConnectionOverviewByDestinationName = (destinationName: string) => {
  clickOnCellInTable(connectionsTable, destinationColumnName, destinationName);
};

export const goToSettingsPage = () => {
  cy.get(settingsTab).click();
};

export const saveChanges = ({ interceptUpdateHandler }: { interceptUpdateHandler?: RouteHandler } = {}) => {
  interceptUpdateConnectionRequest(interceptUpdateHandler);

  submitButtonClick();

  return waitForUpdateConnectionRequest().then((interception) => {
    expect(interception.response?.statusCode).to.eq(200, "response status");
    expect(interception.response?.body).to.exist;
    const connection: WebBackendConnectionRead = interception.response?.body;

    // Your changes were saved!
    return checkSuccessResult().then(() => connection);
  });
};

export const checkSuccessResult = () => cy.contains("Your changes were saved!").should("exist");
