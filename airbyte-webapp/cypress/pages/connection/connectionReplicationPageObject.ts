import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { submitButtonClick } from "commands/common";
import { interceptUpdateConnectionRequest, waitForUpdateConnectionRequest } from "commands/interceptors";
import { RouteHandler } from "cypress/types/net-stubbing";
import { getTestId } from "utils/selectors";

const resetModalSaveButton = "[data-testid='resetModal-save']";
const successResult = "div[data-id='success-result']";
const resetModalResetCheckbox = "[data-testid='resetModal-reset-checkbox']";
const saveStreamChangesButton = "button[data-testid='resetModal-save']";
const schemaChangesDetectedBanner = "[data-testid='schemaChangesDetected']";
const schemaChangesReviewButton = "[data-testid='schemaChangesDetected-button']";
const schemaChangesBackdrop = "[data-testid='schemaChangesBackdrop']";
export const nonBreakingChangesPreference = "[data-testid='nonBreakingChangesPreference-listbox-button']";
const nonBreakingChangesPreferenceValue = (value: string) => `[data-testid='${value}-option']`;
const noDiffToast = "[data-testid='notification-connection.noDiff']";
const cancelButton = getTestId("cancel-edit-button", "button");
const saveButton = getTestId("save-edit-button", "button");
export const refreshSourceSchemaBtn = getTestId("refresh-source-schema-btn", "button");

export const checkSchemaChangesDetected = ({ breaking }: { breaking: boolean }) => {
  cy.get(schemaChangesDetectedBanner).should("exist");
  cy.get(schemaChangesDetectedBanner)
    .invoke("attr", "class")
    .should("match", breaking ? /error/ : /info/);
  cy.get(schemaChangesBackdrop).should(breaking ? "exist" : "not.exist");
};

interface ClickSaveButtonParams {
  expectModal?: boolean;
  shouldReset?: boolean;
  interceptUpdateHandler?: RouteHandler;
}

export const saveChangesAndHandleResetModal = ({
  expectModal = true,
  shouldReset = false,
  interceptUpdateHandler,
}: ClickSaveButtonParams = {}) => {
  interceptUpdateConnectionRequest(interceptUpdateHandler);
  // todo: should we assert status code here?

  submitButtonClick();

  if (expectModal) {
    confirmStreamConfigurationChangedPopup({ reset: shouldReset });
  }

  return waitForUpdateConnectionRequest().then((interception) => {
    expect(interception.response?.statusCode).to.eq(200, "response status");
    expect(interception.response?.body).to.exist;
    const connection: WebBackendConnectionRead = interception.response?.body;

    return checkSuccessResult().then(() => connection);
  });
};

export const checkSuccessResult = () => cy.get(successResult).should("exist");

export const confirmStreamConfigurationChangedPopup = ({ reset = false } = {}) => {
  if (!reset) {
    cy.get(resetModalResetCheckbox).click({ force: true });
  }
  cy.get(saveStreamChangesButton).click();
};

export const checkSchemaChangesDetectedCleared = () => {
  cy.get(schemaChangesDetectedBanner).should("not.exist");
  cy.get(schemaChangesBackdrop).should("not.exist");
};

export const checkNoDiffToast = () => {
  cy.get(noDiffToast).should("exist");
};

export const clickSchemaChangesReviewButton = () => {
  cy.get(schemaChangesReviewButton).click();
  cy.get(schemaChangesReviewButton).should("not.exist");
};

export const selectNonBreakingChangesPreference = (preference: "ignore" | "disable") => {
  cy.get(nonBreakingChangesPreference).click();
  cy.get(nonBreakingChangesPreferenceValue(preference)).click();
};

export const resetModalSaveBtnClick = () => cy.get(resetModalSaveButton).click();

export const clickCancelEditButton = () => {
  cy.get(cancelButton).click({ force: true });
};

export const getSaveButton = () => {
  return cy.get(saveButton);
};

export const clickRefreshSourceSchemaButton = () => {
  cy.get(refreshSourceSchemaBtn).click({ force: true });
};
