import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { submitButtonClick } from "commands/common";
import { interceptUpdateConnectionRequest, waitForUpdateConnectionRequest } from "commands/interceptors";
import { RouteHandler } from "cypress/types/net-stubbing";

const settingsTab = "a[data-testid='settings-step']";
export const resetDataButton = "[data-testid='resetDataButton']";
export const deleteConnectionButton = "[data-testid='open-delete-modal']";

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
