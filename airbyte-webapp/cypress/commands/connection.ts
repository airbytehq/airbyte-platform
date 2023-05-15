import {
  selectSchedule,
  setupDestinationNamespaceSourceFormat,
  enterConnectionName,
} from "pages/connection/connectionFormPageObject";
import { openAddSource } from "pages/destinationPage";

import { submitButtonClick } from "./common";
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
