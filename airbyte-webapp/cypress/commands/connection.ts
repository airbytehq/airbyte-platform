import { ConnectionStatus, WebBackendConnectionRead } from "@src/core/api/generated/AirbyteClient.schemas";
import { WebBackendConnectionCreate, DestinationRead, SourceRead } from "@src/core/api/types/AirbyteClient";

import {
  selectSchedule,
  setupDestinationNamespaceSourceFormat,
  enterConnectionName,
} from "pages/connection/connectionFormPageObject";
import { openAddSource } from "pages/destinationPage";

import {
  getConnectionCreateRequest,
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

  // TODO is this actually needed?
  // eslint-disable-next-line cypress/no-unnecessary-waiting
  cy.wait(5000);

  openAddSource();
  cy.get("div").contains(sourceName).click();
  cy.wait("@discoverSchema");
  enterConnectionName("Connection name");
  selectSchedule("Manual");

  setupDestinationNamespaceSourceFormat();
  submitButtonClick();

  cy.wait("@createConnection", { requestTimeout: 10000 });
};

export const startManualSync = () => {
  cy.get("[data-testid='manual-sync-button']").click();
};

export const createPokeApiSourceViaApi = () => {
  let source: SourceRead;
  return requestWorkspaceId().then(() => {
    const sourceRequestBody = getPokeApiCreateSourceBody(appendRandomString("PokeAPI Source"), "luxray");
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

export const createNewConnectionViaApi = (source: SourceRead, destination: DestinationRead) => {
  let connection: WebBackendConnectionRead;
  let connectionRequestBody: WebBackendConnectionCreate;

  const myConnection = requestWorkspaceId().then(() => {
    requestSourceDiscoverSchema({ sourceId: source.sourceId, disable_cache: true }).then(({ catalog, catalogId }) => {
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
