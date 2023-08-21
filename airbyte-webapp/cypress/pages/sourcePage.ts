import { clickOnCellInTable } from "commands/common";

const newSource = "button[data-id='new-source']";
const sourcesTable = "table[data-testid='sourcesTable']";
const sourceNameColumn = "Name";

export const goToSourcePage = () => {
  cy.intercept("/api/v1/sources/list").as("getSourcesList");
  cy.visit("/source");
};

export const openSourceDestinationFromGrid = (value: string) => {
  cy.get("div").contains(value).click();
};

export const openSourceConnectionsPage = (sourceName: string) => {
  clickOnCellInTable(sourcesTable, sourceNameColumn, sourceName);
  cy.get("a[data-testid='connections-step']").click();
};

export const openNewSourcePage = () => {
  cy.wait("@getSourcesList", { timeout: 30000 }).then(({ response }) => {
    if (response?.body.sources.length) {
      cy.get(newSource).click();
    }
  });
  cy.url().should("include", `/source/new-source`);
};
