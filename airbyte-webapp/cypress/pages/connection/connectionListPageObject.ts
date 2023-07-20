import { WebBackendConnectionListItem } from "@src/core/api/types/AirbyteClient";
import { getWorkspaceId } from "commands/api/workspace";

const statusCell = (connectionId: string) => `[data-testId='statusCell-${connectionId}']`;
const changesStatusIcon = (type: string) => `[data-testId='changesStatusIcon-${type}']`;
const manualSyncButton = "button[data-testId='manual-sync-button']";
const newConnectionButton = "[data-testid='new-connection-button']";

export const visit = () => {
  cy.intercept("**/web_backend/connections/list").as("listConnections");
  cy.visit(`/workspaces/${getWorkspaceId()}/connections`);
  cy.wait("@listConnections", { timeout: 20000 });
};

export const getSchemaChangeIcon = (connection: WebBackendConnectionListItem, type: "breaking" | "non_breaking") =>
  cy.get(`${statusCell(connection.connectionId)} ${changesStatusIcon(type)}`);

export const getManualSyncButton = (connection: WebBackendConnectionListItem) =>
  cy.get(`${statusCell(connection.connectionId)} ${manualSyncButton}`);

export const clickNewConnectionButton = () => {
  cy.get(newConnectionButton).click();
};
