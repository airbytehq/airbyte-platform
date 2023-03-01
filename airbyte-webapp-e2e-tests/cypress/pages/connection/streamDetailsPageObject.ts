import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";
import { getTestIds } from "utils/selectors";
import { SYNC_MODE_STRINGS } from "./streamsTablePageObject/types";

const [
  streamDetails,
  syncStreamSwitch,
  namespace,
  streamName,
  syncMode,
  closeButton,
  streamSourceFieldName,
  streamSourceDataType,
] = getTestIds(
  "stream-details",
  "stream-details-sync-stream-switch",
  "stream-details-namespace",
  "stream-details-stream-name-value",
  "stream-details-sync-mode-value",
  "stream-details-close-button",
  "stream-source-field-name",
  "stream-source-field-data-type"
);

export class StreamDetailsPageObject {
  checkIsOpen() {
    cy.get(streamDetails).should("exist");
  }

  close() {
    cy.get(closeButton).click();
  }

  checkIsClosed() {
    cy.get(streamDetails).should("not.exist");
  }

  enableSyncStream() {
    cy.get(syncStreamSwitch).check({ force: true });
  }

  disableSyncStream() {
    cy.get(syncStreamSwitch).uncheck({ force: true });
  }

  checkSyncStreamEnabled() {
    cy.get(syncStreamSwitch).should("be.checked");
  }

  checkSyncStreamDisabled() {
    cy.get(syncStreamSwitch).should("not.be.checked");
  }

  checkNamespace(value: string) {
    cy.get(namespace).should("contain.text", value);
  }

  checkStreamName(value: string) {
    cy.get(streamName).should("contain.text", value);
  }

  checkSyncMode(sourceSyncMode: SourceSyncMode, destSyncMode: DestinationSyncMode) {
    cy.get(syncMode).should(
      "contain.text",
      `${SYNC_MODE_STRINGS[sourceSyncMode]} | ${SYNC_MODE_STRINGS[destSyncMode]}`
    );
  }

  checkFields(fieldNames: string[], fieldDataTypes: string[]) {
    cy.get(streamSourceFieldName).each(($span, i) => {
      expect($span.text()).to.equal(fieldNames[i]);
    });

    cy.get(streamSourceDataType).each(($span, i) => {
      expect($span.text()).to.equal(fieldDataTypes[i]);
    });
  }

  // checkScrolling() {}

  // selectCursor(fieldName: string) {}

  // checkCursorSelected(fieldName: string) {}

  // selectPrimaryKeys(fieldNames: string[]) {}

  // checkSourceDefinedCursor() {}

  // checkSourceDefinedPrimaryKey() {}
}

export default new StreamDetailsPageObject();
