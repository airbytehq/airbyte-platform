import {
  selectServiceType,
  enterApiKey,
  enterName,
  enterHost,
  enterPort,
  enterDatabase,
  enterUsername,
  enterPassword,
  enterPokemonName,
  enterSchema,
  openOptionalFields,
  selectXmin,
} from "pages/createConnectorPage";

export const fillPostgresForm = (
  name: string,
  host: string,
  port: string,
  database: string,
  username: string,
  password: string,
  schema: string,
  openOptional = false
) => {
  cy.intercept("/api/v1/source_definition_specifications/get").as("getSourceSpecifications");

  selectServiceType("Postgres", "certified");

  if (openOptional) {
    openOptionalFields();
  }
  enterName(name);
  enterHost(host);
  enterPort(port);
  enterDatabase(database);
  enterSchema(schema);
  enterUsername(username);
  enterPassword(password);
  selectXmin();
};

export const fillPokeAPIForm = (name: string, pokeName: string) => {
  cy.intercept("/api/v1/source_definition_specifications/get").as("getSourceSpecifications");

  selectServiceType("PokeAPI", "marketplace");

  enterName(name);
  enterPokemonName(pokeName);
};

export const fillDummyApiForm = (name: string, apiKey: string) => {
  cy.intercept("/api/v1/source_definition_specifications/get").as("getSourceSpecifications");

  selectServiceType(name, "custom");

  enterName(name);
  enterApiKey(apiKey);
};

export const fillE2ETestingForm = (name: string) => {
  cy.intercept("/api/v1/destination_definition_specifications/get").as("getDestinationSpecifications");

  selectServiceType("End-to-End Testing (/dev/null)", "marketplace");

  cy.wait("@getDestinationSpecifications");

  enterName(name);
};
