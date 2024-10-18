import { getNamespaceRowTestId } from "@cy/pages/connection/StreamRowPageObjectV2";

const syncCatalogTable = "table[data-testid='sync-catalog-table']";
const streamNameInput = "input[data-testid='sync-catalog-search']";
const syncNamespaceCheckbox = "input[data-testid='sync-namespace-checkbox']";

// table controls
const refreshSchemaButton = "button[data-testid='refresh-schema-btn']";
const expandCollapseAllStreamsButton = "button[data-testid='expand-collapse-all-streams-btn']";

// filter tabs
const allTab = "button[data-id='all-step']";
const enabledStreamsTab = "button[data-id='enabledstreams-step']";
const disabledStreamsTab = "button[data-id='disabledstreams-step']";
const tabs = {
  all: allTab,
  enabledStreams: enabledStreamsTab,
  disabledStreams: disabledStreamsTab,
};

const headerRow = "thead tr";
const headerRowNamespaceCell = `${headerRow} th`;
const openNamespaceModalGearButton = "button[data-testid='destination-namespace-modal-btn']";

// error messages
const noStreamsSelectedError = "Select at least 1 stream to sync.";

export class StreamsTablePageObjectV2 {
  // empty state
  isNoStreamsMsgDisplayed(expectedResult: boolean) {
    cy.get(syncCatalogTable)
      .contains("No streams")
      .should(expectedResult ? "exist" : "not.exist");
  }

  // in case search is applied
  isNoMatchingStreamsMsgDisplayed(expectedResult: boolean) {
    cy.get(syncCatalogTable)
      .contains("No matching streams")
      .should(expectedResult ? "exist" : "not.exist");
  }

  // table header
  isNamespaceCellEmpty(expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .first()
      .should(expectedResult ? "be.empty" : "not.be.empty");
  }

  isNamespaceCheckboxEnabled(expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .find(syncNamespaceCheckbox)
      .should(expectedResult ? "be.enabled" : "be.disabled");
  }

  isNamespaceCheckboxChecked(expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .find(syncNamespaceCheckbox)
      .should(expectedResult ? "be.checked" : "not.be.checked");
  }

  isNamespaceCheckboxMixed(expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .find(syncNamespaceCheckbox)
      .should(`${expectedResult ? "" : "not."}have.attr`, "aria-checked", "mixed");
  }

  toggleNamespaceCheckbox(namespaceName: string, checked: boolean) {
    cy.get(headerRowNamespaceCell)
      .find(syncNamespaceCheckbox)
      .as("checkbox")
      .then(($checkbox) => {
        checked ? cy.get("@checkbox").check({ force: true }) : cy.get("@checkbox").uncheck({ force: true });
      });
  }

  areAllStreamsInNamespaceEnabled(namespaceName: string, checked: boolean) {
    cy.get(getNamespaceRowTestId(namespaceName))
      .nextAll('[data-testid^="row-depth-1-stream"]')
      .each(($row) => {
        cy.wrap($row)
          .find('[data-testid="sync-stream-checkbox"]')
          .should(checked ? "be.checked" : "not.be.checked");
      });
  }

  isNamespaceNameDisplayed(namespaceName: string, expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .contains(namespaceName)
      .should(expectedResult ? "exist" : "not.exist");
  }

  isTotalAmountOfStreamsDisplayed(amount: number, expectedResult: boolean) {
    cy.get(headerRowNamespaceCell)
      .contains(`${amount} streams`)
      .should(expectedResult ? "exist" : "not.exist");
  }

  isOpenNamespaceModalGearButtonDisplayed(expectedResult: boolean) {
    cy.get(openNamespaceModalGearButton).should(expectedResult ? "exist" : "not.exist");
  }

  isSyncModeColumnNameDisplayed() {
    cy.get(headerRow).contains("Sync mode").should("exist");
  }

  isFieldsColumnNameDisplayed() {
    cy.get(headerRow).contains("Fields").should("exist");
  }

  // filter input
  filterByStreamOrFieldName(value: string) {
    cy.get(streamNameInput).clear();
    cy.get(streamNameInput).type(value);
  }

  clearFilterByStreamOrFieldNameInput() {
    cy.get(streamNameInput).clear();
  }

  isFilterByStreamOrFieldNameInputEnabled(expectedResult: boolean) {
    cy.get(streamNameInput).should(expectedResult ? "be.enabled" : "be.disabled");
  }

  // filter tabs
  clickFilterTab(tabName: "all" | "enabledStreams" | "disabledStreams") {
    cy.get(tabs[tabName]).click();
  }

  isFilterTabEnabled(tabName: "all" | "enabledStreams" | "disabledStreams", expectedResult: boolean) {
    cy.get(tabs[tabName]).should(expectedResult ? "be.enabled" : "be.disabled");
  }

  isFilterTabActive(tabName: "all" | "enabledStreams" | "disabledStreams", expectedResult: boolean) {
    cy.get(tabs[tabName])
      .invoke("attr", "class")
      .should(`${expectedResult ? "" : "not."}match`, new RegExp("active"));
  }

  isRefreshSourceSchemaBtnExist(expectedResult: boolean) {
    cy.get(refreshSchemaButton).should(expectedResult ? "exist" : "not.exist");
  }

  isRefreshSourceSchemaBtnEnabled(expectedResult: boolean) {
    cy.get(refreshSchemaButton).should(expectedResult ? "be.enabled" : "be.disabled");
  }

  clickRefreshSourceSchemaButton() {
    cy.get(refreshSchemaButton).click();
  }

  isToggleExpandCollapseAllStreamsBtnExist(expectedResult: boolean) {
    cy.get(expandCollapseAllStreamsButton).should(expectedResult ? "exist" : "not.exist");
  }

  isToggleExpandCollapseAllStreamsBtnEnabled(expectedResult: boolean) {
    cy.get(expandCollapseAllStreamsButton).should(expectedResult ? "be.enabled" : "be.disabled ");
  }

  toggleExpandCollapseAllStreamsBtn() {
    cy.get(expandCollapseAllStreamsButton).click();
  }

  isNoStreamsSelectedErrorDisplayed(expectedResult: boolean) {
    cy.contains(noStreamsSelectedError).should(expectedResult ? "exist" : "not.exist");
  }
}

export const streamsTableV2 = new StreamsTablePageObjectV2();
