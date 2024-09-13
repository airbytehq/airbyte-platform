import * as statusPage from "@cy/pages/connection/statusPageObject";
import {
  DestinationRead,
  SourceRead,
  WebBackendConnectionCreate,
  ConnectionStatus,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";

import {
  enterConnectionName,
  selectScheduleType,
  setupDestinationNamespaceSourceFormat,
} from "pages/connection/connectionFormPageObject";
import { nextButtonOrLink } from "pages/connection/connectionReplicationPageObject";
import { openCreateConnection } from "pages/destinationPage";

import {
  getConnectionCreateRequest,
  getFakerCreateSourceBody,
  getLocalJSONCreateDestinationBody,
  getPokeApiCreateSourceBody,
  getPostgresCreateDestinationBody,
  getPostgresCreateSourceBody,
  requestCreateConnection,
  requestCreateDestination,
  requestCreateSource,
  requestSourceDiscoverSchema,
  requestWorkspaceId,
} from "./api";
import { appendRandomString, submitButtonClick } from "./common";
import { createLocalJsonDestination, createPostgresDestination } from "./destination";
import { createDummyApiSource, createPokeApiSource, createPostgresSource } from "./source";

export const createTestConnection = (sourceName: string, destinationName: string) => {
  cy.intercept("/api/v1/sources/discover_schema").as("discoverSchema");
  cy.intercept("/api/v1/web_backend/connections/create").as("createConnection");

  switch (true) {
    case sourceName.includes("PokeAPI"):
      createPokeApiSource(sourceName, "luxray");
      break;

    case sourceName.includes("Postgres"):
      createPostgresSource(sourceName);
      break;

    case sourceName.includes("dummy"):
      createDummyApiSource(sourceName);
      break;

    default:
      createPostgresSource(sourceName);
  }

  switch (true) {
    case destinationName.includes("Postgres"):
      createPostgresDestination(destinationName);
      break;
    case destinationName.includes("JSON"):
      createLocalJsonDestination(destinationName);
      break;
    default:
      createLocalJsonDestination(destinationName);
  }

  cy.get("a[data-testid='connections-step']").click();
  openCreateConnection();

  cy.get("div").contains(sourceName).click();
  cy.wait("@discoverSchema", { timeout: 60000 });

  cy.get(nextButtonOrLink).click();

  enterConnectionName("Connection name");
  selectScheduleType("Manual");
  setupDestinationNamespaceSourceFormat(true);

  submitButtonClick();

  cy.wait("@createConnection", { requestTimeout: 10000 });
};

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

export const createJsonDestinationViaApi = () => {
  let destination: DestinationRead;
  return requestWorkspaceId().then(() => {
    const destinationRequestBody = getLocalJSONCreateDestinationBody(appendRandomString("Local JSON Destination"));
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
