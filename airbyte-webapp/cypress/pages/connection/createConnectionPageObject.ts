import { getWorkspaceId } from "@cy/commands/api/workspace";
import { getTestId } from "@cy/utils/selectors";

export type ConnectorType = "source" | "destination";
const getExistingConnectorItemButton = (connectorType: ConnectorType, connectorName: string) =>
  `button[data-testid='select-existing-${connectorType}-${connectorName}']`;

const getExistingConnectorTypeOption = (connectorType: ConnectorType) =>
  `[data-testid='radio-button-tile-${connectorType}Type-existing']`;
const getNewConnectorTypeOption = (connectorType: ConnectorType) =>
  `[data-testid='radio-button-tile-${connectorType}Type-new']`;

export const nextButton = getTestId("next-creation-page");
export const selectExistingConnectorFromList = (connectorType: ConnectorType, connectorName: string) => {
  cy.get(getExistingConnectorItemButton(connectorType, connectorName)).click();
};

export const isExistingConnectorTypeSelected = (connectorType: ConnectorType) => {
  cy.get(getExistingConnectorTypeOption(connectorType)).should("have.attr", "aria-checked", "true");
  cy.get(getNewConnectorTypeOption(connectorType)).should("have.attr", "aria-checked", "false");
};

export const isNextPageButtonEnabled = (expectedResult: boolean) => {
  cy.get(nextButton).should(expectedResult ? "not.be.disabled" : "be.disabled");
};

export const isAtConnectionConfigurationStep = () =>
  cy.url().should("include", `/workspaces/${getWorkspaceId()}/connections/new-connection/configure`);

export const isAtConnectionOverviewPage = (connectionId: string) =>
  cy.url().should("include", `/workspaces/${getWorkspaceId()}/connections/${connectionId}/status`);
