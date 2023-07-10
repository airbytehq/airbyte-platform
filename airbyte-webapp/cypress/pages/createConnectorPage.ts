const nameInput = "input[name=name]";
const apiKeyInput = "input[name='connectionConfiguration.api_key']";
const hostInput = "input[name='connectionConfiguration.host']";
const portInput = "input[name='connectionConfiguration.port']";
const databaseInput = "input[name='connectionConfiguration.database']";
const usernameInput = "input[name='connectionConfiguration.username']";
const passwordInput = "input[name='connectionConfiguration.password']";
const pokemonNameInput = "input[name='connectionConfiguration.pokemon_name']";
const schemaInput = "[data-testid='tag-input'] input";
const destinationPathInput = "input[name='connectionConfiguration.destination_path']";
const optionalFieldsButton = "button[data-testid='optional-fields']";

export const selectServiceType = (type: string) => {
  // Make sure alpha connectors are visible in the grid, since they are hidden by default
  cy.contains("label", "Alpha").click();
  cy.contains("button", type).click();
};

export const enterName = (name: string) => {
  cy.get(nameInput).clear();
  cy.get(nameInput).type(name);
};

export const enterApiKey = (apiKey: string) => {
  cy.get(apiKeyInput).type(apiKey);
};

export const enterHost = (host: string) => {
  cy.get(hostInput).type(host);
};

export const enterPort = (port: string) => {
  cy.get(portInput).type("{selectAll}{del}");
  cy.get(portInput).type(port);
};

export const enterDatabase = (database: string) => {
  cy.get(databaseInput).type(database);
};

export const enterUsername = (username: string) => {
  cy.get(usernameInput).type(username);
};

export const enterPassword = (password: string) => {
  cy.get(passwordInput).type(password);
};

export const enterPokemonName = (pokeName: string) => {
  cy.get(pokemonNameInput).type(pokeName);
};

export const enterDestinationPath = (destinationPath: string) => {
  cy.get(destinationPathInput).type(destinationPath);
};

export const enterSchema = (value: string) => {
  if (!value) {
    return;
  }
  cy.get(schemaInput).first().type(value, { force: true });
  cy.get(schemaInput).first().type("{enter}", { force: true });
};

export const removeSchema = (value = "Remove public") => {
  if (!value) {
    return;
  }
  cy.get(`[aria-label*="${value}"]`).click();
};

export const openOptionalFields = () => {
  cy.get(optionalFieldsButton).click();
};
