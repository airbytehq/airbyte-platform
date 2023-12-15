import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "@cy/commands/api";
import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
  startManualReset,
  startManualSync,
} from "@cy/commands/connection";
import { runDbQuery } from "@cy/commands/db/db";
import { createUsersTableQuery, dropUsersTableQuery } from "@cy/commands/db/queries";
import { DestinationRead, SourceRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";
import { ConnectionRoutePaths, RoutePaths } from "@src/pages/routePaths";

import * as statusPage from "pages/connection/statusPageObject";

describe("Status tab", () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;

  before(() => {
    runDbQuery(dropUsersTableQuery);
    runDbQuery(createUsersTableQuery);

    createPostgresSourceViaApi().then((source) => {
      postgresSource = source;
    });
    createPostgresDestinationViaApi().then((destination) => {
      postgresDestination = destination;
    });
  });

  beforeEach(() => {
    createNewConnectionViaApi(postgresSource, postgresDestination).as("connection");
  });

  afterEach(() => {
    cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
      requestDeleteConnection({ connectionId: connection.connectionId });
    });
  });

  after(() => {
    if (postgresSource) {
      requestDeleteSource({ sourceId: postgresSource.sourceId });
    }

    if (postgresDestination) {
      requestDeleteDestination({
        destinationId: postgresDestination.destinationId,
      });
    }

    runDbQuery(dropUsersTableQuery);
  });

  it("should initialize as pending", () => {
    cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
      cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Status}/`);
      cy.get(statusPage.connectionStatusText).contains("Pending").should("exist");
    });
  });

  it("should allow starting a sync", () => {
    cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
      cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Status}/`);

      // sync & verify the button enters and exits its disabled state as the status updates
      startManualSync();
      cy.get(statusPage.manualSyncButton).should("be.disabled");

      // Wait for the job to start
      cy.get(`[data-loading="true"]`, { timeout: 10000 }).should("exist");

      // Wait for the job to complete
      cy.get(`[data-loading="false"]`, { timeout: 45000 }).should("exist");

      cy.get(statusPage.manualSyncButton).should("not.be.disabled");
    });
  });

  it("should allow resetting a sync", () => {
    cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
      cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Status}/`);

      // reset & verify the button enters and exits its disabled state as the status updates
      startManualReset();

      cy.get(statusPage.jobHistoryDropdownMenu).click();
      cy.get(statusPage.resetDataDropdownOption).should("be.disabled");
      // Click again to close the menu
      cy.get(statusPage.jobHistoryDropdownMenu).click();

      // Wait for the job to start
      cy.get(`[data-loading="true"]`, { timeout: 10000 }).should("exist");

      // Wait for the job to complete
      cy.get(`[data-loading="false"]`, { timeout: 45000 }).should("exist");

      cy.get(statusPage.jobHistoryDropdownMenu).click();
      cy.get(statusPage.resetDataDropdownOption).should("not.be.disabled");
    });
  });
});
