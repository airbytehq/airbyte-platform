import * as statusPage from "@cy/pages/connection/statusPageObject";
import {
  DestinationRead,
  SourceRead,
  WebBackendConnectionCreate,
  ConnectionStatus,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";

import {
  getConnectionCreateRequest,
  getE2ETestingCreateDestinationBody,
  getFakerCreateSourceBody,
  getPokeApiCreateSourceBody,
  getPostgresCreateDestinationBody,
  getPostgresCreateSourceBody,
  requestCreateConnection,
  requestCreateDestination,
  requestCreateSource,
  requestSourceDiscoverSchema,
  requestWorkspaceId,
} from "./api";
import { appendRandomString } from "./common";

export const startManualSync = () => {
  cy.get("[data-testid='manual-sync-button']").click();
};

export const startManualReset = () => {
  cy.get(statusPage.jobHistoryDropdownMenu).click();
  cy.get(statusPage.resetDataDropdownOption).click();
  cy.get("[data-id='clear-data']").click();
};

export const createFakerSourceViaApi = () => {
  let source: SourceRead;
  return requestWorkspaceId().then(() => {
    const sourceRequestBody = getFakerCreateSourceBody(appendRandomString("Faker Source"));
    requestCreateSource(sourceRequestBody).then((sourceResponse) => {
      source = sourceResponse;
    });
    return source;
  });
};

export const createPokeApiSourceViaApi = () => {
  let source: SourceRead;
  return requestWorkspaceId().then(() => {
    const sourceRequestBody = getPokeApiCreateSourceBody(appendRandomString("PokeAPI Source"), "venusaur");
    requestCreateSource(sourceRequestBody).then((sourceResponse) => {
      source = sourceResponse;
    });
    return source;
  });
};

export const createPostgresSourceViaApi = () => {
  let source: SourceRead;
  const mySource = requestWorkspaceId().then(() => {
    const sourceRequestBody = getPostgresCreateSourceBody(appendRandomString("Postgres Source"));
    requestCreateSource(sourceRequestBody).then((sourceResponse) => {
      source = sourceResponse;
    });
    return source;
  });
  return mySource;
};

export const createE2ETestingDestinationViaApi = () => {
  let destination: DestinationRead;
  return requestWorkspaceId().then(() => {
    const destinationRequestBody = getE2ETestingCreateDestinationBody(
      appendRandomString("End-to-End Testing (/dev/null) Destination")
    );
    requestCreateDestination(destinationRequestBody).then((destinationResponse) => {
      destination = destinationResponse;
    });
    return destination;
  });
};

export const createPostgresDestinationViaApi = () => {
  let destination: DestinationRead;
  return requestWorkspaceId().then(() => {
    const destinationRequestBody = getPostgresCreateDestinationBody(appendRandomString("Postgres Destination"));
    requestCreateDestination(destinationRequestBody).then((destinationResponse) => {
      destination = destinationResponse;
    });
    return destination;
  });
};

export const createNewConnectionViaApi = (
  source: SourceRead,
  destination: DestinationRead,
  options: { enableAllStreams?: boolean } = {}
) => {
  let connection: WebBackendConnectionRead;
  let connectionRequestBody: WebBackendConnectionCreate;

  const myConnection = requestWorkspaceId().then(() => {
    requestSourceDiscoverSchema({
      sourceId: source.sourceId,
      disable_cache: true,
    }).then(({ catalog, catalogId }) => {
      if (options.enableAllStreams) {
        catalog?.streams.forEach((stream) => {
          if (stream.config) {
            stream.config.selected = true;
          }
        });
      }
      connectionRequestBody = getConnectionCreateRequest({
        name: appendRandomString(`${source.name} → ${destination.name} Cypress Connection`),
        sourceId: source.sourceId,
        destinationId: destination.destinationId,
        syncCatalog: catalog,
        sourceCatalogId: catalogId,
        status: ConnectionStatus.active,
      });
      requestCreateConnection(connectionRequestBody).then((connectionResponse) => {
        connection = connectionResponse;
      });
    });
    return connection;
  });
  return myConnection;
};
