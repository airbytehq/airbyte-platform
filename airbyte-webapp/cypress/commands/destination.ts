import { goToDestinationPage, openNewDestinationForm } from "pages/destinationPage";

import { deleteEntity, openConnectorPage, submitButtonClick, updateField } from "./common";
import { fillE2ETestingForm, fillPostgresForm } from "./connector";

export const createE2ETestingDestination = (name: string) => {
  cy.intercept("/api/v1/scheduler/destinations/check_connection").as("checkDestinationConnection");
  cy.intercept("/api/v1/destinations/create").as("createDestination");

  goToDestinationPage();
  openNewDestinationForm();
  fillE2ETestingForm(name);
  submitButtonClick();

  cy.wait("@checkDestinationConnection", { requestTimeout: 60000 });
  cy.wait("@createDestination");
};

export const createPostgresDestination = (
  name: string,
  host = Cypress.env("DESTINATION_DB_HOST") || "localhost",
  port = "5434",
  database = "airbyte_ci_destination",
  username = "postgres",
  password = "secret_password",
  schema = ""
) => {
  cy.intercept("/api/v1/scheduler/destinations/check_connection").as("checkDestinationConnection");
  cy.intercept("/api/v1/destinations/create").as("createDestination");

  goToDestinationPage();
  openNewDestinationForm();
  fillPostgresForm(name, host, port, database, username, password, schema, true);
  submitButtonClick();

  cy.wait("@checkDestinationConnection", { requestTimeout: 30000 });
  cy.wait("@createDestination");
};

export const updateDestination = (name: string, field: string, value: string) => {
  cy.intercept("/api/v1/destinations/check_connection_for_update").as("checkDestinationUpdateConnection");
  cy.intercept("/api/v1/destinations/update").as("updateDestination");

  goToDestinationPage();
  openConnectorPage(name);
  updateField(field, value);
  submitButtonClick();

  cy.wait("@checkDestinationUpdateConnection");
  cy.wait("@updateDestination");
};

export const deleteDestination = (name: string) => {
  cy.intercept("/api/v1/destinations/delete").as("deleteDestination");
  goToDestinationPage();
  openConnectorPage(name);
  deleteEntity();
  cy.wait("@deleteDestination");
};
