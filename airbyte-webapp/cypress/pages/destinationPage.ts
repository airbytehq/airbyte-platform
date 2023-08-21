import { clickOnCellInTable } from "@cy/commands/common";

const newDestination = "button[data-id='new-destination']";
const addSourceButton = "button[data-testid='select-source']";
const createConnectionButton = "button[data-testid='create-connection']";

const destinationsTable = "table[data-testid='destinationsTable']";
const destinationNameColumn = "Name";

export const goToDestinationPage = () => {
  cy.intercept("/api/v1/destinations/list").as("getDestinationsList");
  cy.visit("/destination");
};

export const openDestinationConnectionsPage = (destinationName: string) => {
  clickOnCellInTable(destinationsTable, destinationNameColumn, destinationName);
  cy.get("a[data-testid='connections-step']").click();
};

export const openNewDestinationForm = () => {
  cy.wait("@getDestinationsList", { timeout: 30000 }).then(({ response }) => {
    if (response?.body.destinations.length) {
      cy.get(newDestination).click();
    }
  });
  cy.url().should("include", `/destination/new-destination`);
};

export const openAddSource = () => {
  cy.get(addSourceButton).click();
};

export const openCreateConnection = () => {
  cy.get(createConnectionButton).click();
};
