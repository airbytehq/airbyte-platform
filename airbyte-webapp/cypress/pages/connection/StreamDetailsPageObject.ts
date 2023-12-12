import { DestinationSyncMode, SyncMode } from "@src/core/api/types/AirbyteClient";
import { getTestId, getTestIds } from "utils/selectors";

import { syncModeSelectButton } from "./StreamRowPageObject";
import { SYNC_MODE_STRINGS } from "./types";

const [
  streamDetailsPanel,
  syncStreamSwitch,
  namespace,
  streamName,
  closeButton,
  streamSourceFieldName,
  streamSourceDataType,
  cursorRadioButton,
  primaryKeyCheckbox,
  destinationFieldName,
  syncFieldSwitch,
] = getTestIds(
  "stream-details",
  "stream-details-sync-stream-switch",
  "stream-details-namespace",
  "stream-details-stream-name-value",
  "stream-details-close-button",
  "stream-source-field-name",
  "stream-source-field-data-type",
  "field-cursor-radio-button",
  "field-primary-key-checkbox",
  "stream-destination-field-name",
  "sync-field-switch"
);

const getFieldTableRowTestId = (rowIndex: number) => getTestId(`table-row-${rowIndex}`);

const getRowByFieldName = (name: string) => cy.get(streamSourceFieldName).contains(name).parents("tr").first();

const verifyCursor = (cursor: string | undefined, fieldName: string, hasSourceDefinedCursor?: boolean) => {
  // Does not support nested property cases
  // (e.g metaData is disabled because it has properties such as metaData.size)
  if (cursor) {
    cy.get(cursorRadioButton)
      .should(`${cursor === fieldName ? "" : "not."}be.checked`)
      .should(`${hasSourceDefinedCursor ? "" : "not."}be.disabled`);
  } else {
    cy.get(cursorRadioButton).should("not.exist");
  }
};

const verifyPrimaryKey = (
  primaryKeys: string[] | undefined,
  fieldName: string,
  hasSourceDefinedPrimaryKeys?: boolean
) => {
  // Does not support nested property cases
  // (e.g metaData is disabled because it has properties such as metaData.size)
  if (primaryKeys) {
    cy.get(primaryKeyCheckbox)
      .should(`${primaryKeys.includes(fieldName) ? "" : "not."}be.checked`)
      .should(`${hasSourceDefinedPrimaryKeys ? "" : "not."}be.disabled`);
  } else {
    cy.get(primaryKeyCheckbox).should("not.exist");
  }
};

interface AreFieldsValidParams {
  names: string[];
  dataTypes: string[];
  cursor?: string;
  hasSourceDefinedCursor?: boolean;
  primaryKeys?: string[];
  hasSourceDefinedPrimaryKeys?: boolean;
}

export class StreamDetailsPageObject {
  isOpen() {
    cy.get(streamDetailsPanel).should("exist");
  }

  close() {
    cy.get(closeButton).click();
  }

  isClosed() {
    cy.get(streamDetailsPanel).should("not.exist");
  }

  scrollToBottom() {
    return cy.get(streamDetailsPanel).within(() => {
      cy.get("div[data-test-id='virtuoso-scroller']").scrollTo("bottom");
    });
  }

  scrollToTop() {
    return cy.get(streamDetailsPanel).within(() => {
      cy.get("div[data-test-id='virtuoso-scroller']").scrollTo("top");
    });
  }

  enableSyncStream() {
    cy.get(syncStreamSwitch).check({ force: true });
  }

  disableSyncStream() {
    cy.get(syncStreamSwitch).uncheck({ force: true });
  }

  isSyncStreamEnabled() {
    cy.get(syncStreamSwitch).should("be.checked");
  }

  isSyncStreamDisabled() {
    cy.get(syncStreamSwitch).should("not.be.checked");
  }

  isNamespace(value: string) {
    cy.get(namespace).should("contain.text", value);
  }

  isStreamName(value: string) {
    cy.get(streamName).should("contain.text", value);
  }

  areFieldsValid({
    names,
    dataTypes,
    cursor,
    hasSourceDefinedCursor,
    primaryKeys,
    hasSourceDefinedPrimaryKeys,
  }: AreFieldsValidParams) {
    expect(names.length).to.equal(dataTypes.length, "field name and data type length");
    cy.get(streamSourceFieldName).should("have.length", names.length);

    names.forEach((name, index) => {
      const dataType = dataTypes[index];
      const rowTestId = getFieldTableRowTestId(index);

      cy.get(rowTestId).within(() => {
        cy.get(streamSourceFieldName).should("have.text", name);
        cy.get(streamSourceDataType).should("have.text", dataType);

        verifyCursor(cursor, name, hasSourceDefinedCursor);
        verifyPrimaryKey(primaryKeys, name, hasSourceDefinedPrimaryKeys);

        cy.get(destinationFieldName).should("have.text", name);
      });
    });
  }

  selectSyncMode(sourceSyncMode: SyncMode, destSyncMode: DestinationSyncMode) {
    // todo: this is targeting every sync mode select, not just the one within the panel
    cy.get(streamDetailsPanel).within(() => {
      cy.get(syncModeSelectButton).click({ force: true });
      cy.get('li[role="option"]')
        .contains(`${SYNC_MODE_STRINGS[sourceSyncMode]} | ${SYNC_MODE_STRINGS[destSyncMode]}`)
        .click();
    });
  }

  selectCursor(fieldName: string) {
    getRowByFieldName(fieldName).within(() => {
      cy.get(cursorRadioButton).parent().click({ scrollBehavior: false });
      cy.get(cursorRadioButton).should("be.checked");
    });
  }

  isCursorSelected(fieldName: string) {
    getRowByFieldName(fieldName).within(() => {
      cy.get(cursorRadioButton).should("be.checked");
    });
  }

  hasSourceDefinedCursor(cursor: string) {
    getRowByFieldName(cursor).within(() => {
      verifyCursor(cursor, cursor, true);
    });
  }

  selectPrimaryKeys(fieldNames: string[]) {
    fieldNames.forEach((name) => {
      getRowByFieldName(name).within(() => {
        cy.get(primaryKeyCheckbox).parent().click();
        cy.get(primaryKeyCheckbox).should("be.checked");
      });
    });
  }

  deSelectPrimaryKeys(fieldNames: string[]) {
    fieldNames.forEach((name) => {
      getRowByFieldName(name).within(() => {
        cy.get(primaryKeyCheckbox).parent().click();
        cy.get(primaryKeyCheckbox).should("not.be.checked");
      });
    });
  }

  arePrimaryKeysSelected(fieldNames: string[]) {
    fieldNames.forEach((name) => {
      getRowByFieldName(name).within(() => {
        cy.get(primaryKeyCheckbox).should("be.checked");
      });
    });
  }

  isSourceDefinedCursor(fieldName: string) {
    getRowByFieldName(fieldName).within(() => {
      cy.get(cursorRadioButton).should("be.checked").should("be.disabled");
    });
  }

  areSourceDefinedPrimaryKeys(fieldNames: string[]) {
    fieldNames.forEach((name) => {
      getRowByFieldName(name).within(() => {
        verifyPrimaryKey(fieldNames, name, true);
      });
    });
  }

  areFieldsDeselected() {
    cy.get(syncFieldSwitch).each(($el) => {
      cy.wrap($el).should("not.be.checked");
    });
  }
}

export const streamDetails = new StreamDetailsPageObject();
