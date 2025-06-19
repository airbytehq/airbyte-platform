import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "@cy/commands/api";
import {
  startManualSync,
  createNewConnectionViaApi,
  createPokeApiSourceViaApi,
  createE2ETestingDestinationViaApi,
} from "@cy/commands/connection";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { DestinationRead, SourceRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

describe("Connection Timeline", () => {
  let pokeApiSource: SourceRead;
  let E2ETestingDestination: DestinationRead;
  let connection: WebBackendConnectionRead | null = null;

  beforeEach(() => {
    createPokeApiSourceViaApi().then((source) => {
      pokeApiSource = source;
    });
    createE2ETestingDestinationViaApi().then((destination) => {
      E2ETestingDestination = destination;
    });
  });

  afterEach(() => {
    if (connection) {
      requestDeleteConnection({ connectionId: connection.connectionId });
      connection = null;
    }
    if (pokeApiSource) {
      requestDeleteSource({ sourceId: pokeApiSource.sourceId });
    }
    if (E2ETestingDestination) {
      requestDeleteDestination({
        destinationId: E2ETestingDestination.destinationId,
      });
    }
  });

  it("Should list events and interact with job logs modal and links", () => {
    cy.visit("/");

    createNewConnectionViaApi(pokeApiSource, E2ETestingDestination).then((connectionResponse) => {
      connection = connectionResponse;
      visit(connection, "timeline");
    });

    cy.contains("No events to display").should("exist");

    startManualSync();
    cy.contains("manually started a sync").should("exist");
    cy.contains("Sync running").should("exist");

    cy.get('[data-testid="cancel-sync-button"]').click();
    cy.contains("Yes, cancel sync").click();
    cy.contains("Sync cancelled").should("exist");
    // copying link from timeline works
    cy.get('[data-testid="job-event-menu"]').should("exist").find("button").should("exist").click();
    cy.get('[data-testid="copy-link-to-event"]').click({ force: true });

    cy.window().then((win) => {
      return win.navigator.clipboard.readText().then((result: string) => cy.visit(result));
    });
    cy.get('[data-testid="job-logs-modal"]', { timeout: 30000 }).should("exist");
    cy.get('[data-testid="close-modal-button"]').click();

    // view logs from menu + copying link from modal works
    cy.get('[data-testid="job-event-menu"]').should("exist").find("button").should("exist").click();
    cy.get('[data-testid="view-logs"]').click();
    cy.get('[data-testid="job-logs-modal"]').should("exist");
    cy.get('[data-testid="copy-link-to-attempt-button"]').click();
    cy.get('[data-testid="close-modal-button"]').click();
    cy.window().then((win) => {
      return win.navigator.clipboard.readText().then((result: string) => cy.visit(result));
    });
    cy.get('[data-testid="job-logs-modal"]', { timeout: 30000 }).should("exist");
    cy.get('[data-testid="close-modal-button"]').click();
  });
});
