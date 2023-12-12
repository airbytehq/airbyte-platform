import { goToSourcePage, openNewSourcePage } from "pages/sourcePage";

import { deleteEntity, openConnectorPage, submitButtonClick, updateField } from "./common";
import { fillDummyApiForm, fillPokeAPIForm, fillPostgresForm } from "./connector";

export const createPostgresSource = (
  name: string,
  host = Cypress.env("SOURCE_DB_HOST") || "localhost",
  port = Cypress.env("SOURCE_DB_PORT") || "5433",
  database = "airbyte_ci_source",
  username = "postgres",
  password = "secret_password",
  schema = ""
) => {
  cy.intercept("/api/v1/scheduler/sources/check_connection").as("checkSourceUpdateConnection");
  cy.intercept("/api/v1/sources/create").as("createSource");

  goToSourcePage();
  openNewSourcePage();
  fillPostgresForm(name, host, port, database, username, password, schema);
  submitButtonClick();

  cy.wait("@checkSourceUpdateConnection", { requestTimeout: 15000 });
  cy.wait("@createSource");
};

export const createPokeApiSource = (name: string, pokeName: string) => {
  cy.intercept("/api/v1/scheduler/sources/check_connection").as("checkSourceUpdateConnection");
  cy.intercept("/api/v1/sources/create").as("createSource");

  goToSourcePage();
  openNewSourcePage();
  fillPokeAPIForm(name, pokeName);
  submitButtonClick();

  cy.wait("@checkSourceUpdateConnection");
  cy.wait("@createSource");
};

export const createDummyApiSource = (name: string) => {
  cy.intercept("/api/v1/scheduler/sources/check_connection").as("checkSourceUpdateConnection");
  cy.intercept("/api/v1/sources/create").as("createSource");

  goToSourcePage();
  openNewSourcePage();
  fillDummyApiForm(name, "theauthkey");
  submitButtonClick();

  cy.wait("@checkSourceUpdateConnection", { timeout: 60000 });
  cy.wait("@createSource");
};

export const updateSource = (name: string, field: string, value: string, isDropdown = false) => {
  cy.intercept("/api/v1/sources/check_connection_for_update").as("checkSourceConnection");
  cy.intercept("/api/v1/sources/update").as("updateSource");

  goToSourcePage();
  openConnectorPage(name);
  updateField(field, value, isDropdown);
  submitButtonClick();

  cy.wait("@checkSourceConnection");
  cy.wait("@updateSource");
};

export const deleteSource = (name: string) => {
  cy.intercept("/api/v1/sources/delete").as("deleteSource");
  goToSourcePage();
  openConnectorPage(name);
  deleteEntity();
  cy.wait("@deleteSource");
};
