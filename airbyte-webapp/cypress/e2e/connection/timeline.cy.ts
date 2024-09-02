import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "@cy/commands/api";
import {
  startManualSync,
  createJsonDestinationViaApi,
  createNewConnectionViaApi,
  createPokeApiSourceViaApi,
} from "@cy/commands/connection";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { setFeatureFlags } from "@cy/support/e2e";
import { DestinationRead, SourceRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

describe("Connection Timeline", () => {
  let pokeApiSource: SourceRead;
  let jsonDestination: DestinationRead;
  let connection: WebBackendConnectionRead | null = null;

  beforeEach(() => {
    setFeatureFlags({ "connection.timeline": true });

    createPokeApiSourceViaApi().then((source) => {
      pokeApiSource = source;
    });
    createJsonDestinationViaApi().then((destination) => {
      jsonDestination = destination;
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
    if (jsonDestination) {
      requestDeleteDestination({
        destinationId: jsonDestination.destinationId,
      });
    }
    setFeatureFlags({});
  });

  it("Should show correct events for empty, started, running, and cancelled jobs", () => {
    cy.visit("/");

    createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
      connection = connectionResponse;
      visit(connection, "timeline");
    });

    cy.contains("No events to display").should("exist");

    startManualSync();

    cy.contains("manually started a sync").should("exist");
  });
});
