export type ConnectorType = "source" | "destination";
const getExistingConnectorItemButton = (connectorType: ConnectorType, connectorName: string) =>
  `button[data-testid='select-existing-${connectorType}-${connectorName}']`;

const getExistingConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-existing']`;
const getNewConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-new']`;

const catalogTreeTableHeader = `div[data-testid='catalog-tree-table-header']`;
const catalogTreeTableBody = `div[data-testid='catalog-tree-table-body']`;

export const selectExistingConnectorFromList = (connectorType: ConnectorType, connectorName: string) => {
  cy.get(getExistingConnectorItemButton(connectorType, connectorName)).click();
};

export const isExistingConnectorTypeSelected = (connectorType: ConnectorType) => {
  cy.get(getExistingConnectorTypeOption(connectorType)).should("be.checked");
  cy.get(getNewConnectorTypeOption(connectorType)).should("not.be.checked");
};

export const isNewConnectorTypeSelected = (connectorType: ConnectorType) => {
  cy.get(getExistingConnectorTypeOption(connectorType)).should("not.be.checked");
  cy.get(getNewConnectorTypeOption(connectorType)).should("be.checked");
};

export const getNoStreamsSelectedError = () => cy.contains("Select at least 1 stream to sync.");
/*
 Route checking
 */
export const isAtNewConnectionPage = () => cy.url().should("include", `/connections/new-connection`);

export const isAtConnectionConfigurationStep = () =>
  cy.url().should("include", `/connections/new-connection/configure`);
export const isAtConnectionOverviewPage = (connectionId: string) =>
  cy.url().should("include", `connections/${connectionId}/status`);

/*
  Stream table
 */

export const checkColumnNames = () => {
  const columnNames = ["Sync", "Data destination", "Stream", "Sync mode"];
  cy.get(catalogTreeTableHeader).within(($header) => {
    columnNames.forEach((columnName) => {
      cy.contains(columnName);
    });
  });
};

export const checkAmountOfStreamTableRows = (expectedAmountOfRows: number) =>
  cy
    .get(catalogTreeTableBody)
    .find("[data-testid^='catalog-tree-table-row-']")
    .should("have.length", expectedAmountOfRows);

export const scrollTableToStream = (streamName: string) => {
  cy.get(catalogTreeTableBody).contains(streamName).scrollIntoView();
};

export const isStreamTableRowVisible = (streamName: string) =>
  cy.get(catalogTreeTableBody).contains(streamName).should("be.visible");
