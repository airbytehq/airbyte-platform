import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { getWorkspaceId } from "commands/api/workspace";
import { interceptGetConnectionRequest, waitForGetConnectionRequest } from "commands/interceptors";
import { RouteHandler } from "cypress/types/net-stubbing";

const replicationTab = "div[data-id='replication-step']";
const syncEnabledSwitch = "[data-testid='enabledControl-switch']";

interface VisitOptions {
  interceptGetHandler?: RouteHandler;
}

export const visit = (connection: WebBackendConnectionRead, tab = "", { interceptGetHandler }: VisitOptions = {}) => {
  interceptGetConnectionRequest(interceptGetHandler);

  cy.visit(`/workspaces/${getWorkspaceId()}/connections/${connection.connectionId}/${tab}`);

  waitForGetConnectionRequest();
};

export const goToReplicationTab = () => {
  cy.get(replicationTab).click();
};

export const getSyncEnabledSwitch = () => cy.get(syncEnabledSwitch);
