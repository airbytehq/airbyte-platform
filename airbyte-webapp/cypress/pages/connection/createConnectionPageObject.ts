export type ConnectorType = "source" | "destination";
const getExistingConnectorItemButton = (connectorType: ConnectorType, connectorName: string) =>
  `button[data-testid='select-existing-${connectorType}-${connectorName}']`;

const getExistingConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-existing']`;
const getNewConnectorTypeOption = (connectorType: ConnectorType) =>
  `input[data-testid='radio-button-tile-${connectorType}Type-new']`;

const connectorHeaderGroupIcon = (connectorType: ConnectorType) =>
  `span[data-testid='connector-header-group-icon-container-${connectorType}']`;
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
export const checkConnectorIconAndTitle = (connectorType: ConnectorType) => {
  const connectorIcon = connectorHeaderGroupIcon(connectorType);
  cy.get(connectorIcon)
    .contains(connectorType, { matchCase: false })
    .within(() => {
      cy.get("img").should("have.attr", "src").should("not.be.empty");
    });
};

export const checkColumnNames = () => {
  const columnNames = ["Sync", "Namespace", "Stream name", "Sync mode", "Cursor field", "Primary key"];
  cy.get(catalogTreeTableHeader).within(($header) => {
    columnNames.forEach((columnName) => {
      cy.contains(columnName);
    });
    // we have two Namespace columns
    cy.get(`div:contains(${columnNames[1]})`).should("have.length", 2);
    // we have two Stream Name columns
    cy.get(`div:contains(${columnNames[2]})`).should("have.length", 2);
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
