import { WebBackendConnectionListItem } from "@src/core/api/types/AirbyteClient";
import { getWorkspaceId } from "commands/api/workspace";

const schemaChangeCell = (connectionId: string) => `[data-testid='link-replication-${connectionId}']`;

const changesStatusIcon = (type: string) => `[data-testId='changesStatusIcon-${type}']`;
const connectionStateSwitch = (connectionId: string) => `[data-testId='connection-state-switch-${connectionId}']`;
const newConnectionButton = "[data-testid='new-connection-button']";

export const visit = () => {
  cy.intercept("**/web_backend/connections/list").as("listConnections");
  cy.visit(`/workspaces/${getWorkspaceId()}/connections`);
  cy.wait("@listConnections", { timeout: 20000 });
};

export const getSchemaChangeIcon = (connection: WebBackendConnectionListItem, type: "breaking" | "non_breaking") =>
  cy.get(`${schemaChangeCell(connection.connectionId)} ${changesStatusIcon(type)}`);

export const getConnectionStateSwitch = (connection: WebBackendConnectionListItem) =>
  cy.get(`${connectionStateSwitch(connection.connectionId)}`);

export const clickNewConnectionButton = () => {
  cy.get(newConnectionButton).click();
};
