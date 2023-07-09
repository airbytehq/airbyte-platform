import { RouteHandler } from "cypress/types/net-stubbing";

export const interceptGetConnectionRequest = (response?: RouteHandler) =>
  cy.intercept("/api/v1/web_backend/connections/get", response).as("getConnection");
export const waitForGetConnectionRequest = () => cy.wait("@getConnection", { timeout: 20000 });

export const interceptUpdateConnectionRequest = (response?: RouteHandler) =>
  cy.intercept("/api/v1/web_backend/connections/update", response).as("updateConnection");
export const waitForUpdateConnectionRequest = () => cy.wait("@updateConnection", { timeout: 10000 });

export const interceptDiscoverSchemaRequest = () =>
  cy.intercept("/api/v1/sources/discover_schema").as("discoverSchema");
export const waitForDiscoverSchemaRequest = () => cy.wait("@discoverSchema");

export const interceptCreateConnectionRequest = () =>
  cy.intercept("/api/v1/web_backend/connections/create").as("createConnection");
export const waitForCreateConnectionRequest = () => cy.wait("@createConnection", { timeout: 20000 });

export const interceptGetSourcesListRequest = () => cy.intercept("/api/v1/sources/list").as("getSourcesList");
export const waitForGetSourcesListRequest = () => cy.wait("@getSourcesList");

export const interceptGetSourceDefinitionsRequest = () =>
  cy.intercept("/api/v1/source_definitions/list_for_workspace").as("getSourceDefinitions");
export const waitForGetSourceDefinitionsRequest = () => cy.wait("@getSourceDefinitions");
