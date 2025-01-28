import { getTestId } from "@cy/utils/selectors";

export type ConnectorType = "source" | "destination";
const getExistingConnectorItemButton = (connectorType: ConnectorType, connectorName: string) =>
  `button[data-testid='select-existing-${connectorType}-${connectorName}']`;

const getExistingConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-existing']`;
const getNewConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-new']`;

export const nextButton = getTestId("next-creation-page");
export const selectExistingConnectorFromList = (connectorType: ConnectorType, connectorName: string) => {
  cy.get(getExistingConnectorItemButton(connectorType, connectorName)).click();
};

export const isExistingConnectorTypeSelected = (connectorType: ConnectorType) => {
  cy.get(getExistingConnectorTypeOption(connectorType)).should("be.checked");
  cy.get(getNewConnectorTypeOption(connectorType)).should("not.be.checked");
};

export const isNextPageButtonEnabled = (expectedResult: boolean) => {
  cy.get(nextButton).should(expectedResult ? "not.be.disabled" : "be.disabled");
};

export const isAtConnectionConfigurationStep = () =>
  cy.url().should("include", `/connections/new-connection/configure`);

export const isAtConnectionOverviewPage = (connectionId: string) =>
  cy.url().should("include", `connections/${connectionId}/status`);
