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

class StreamDetailsPageObject {
  isOpen() {
    cy.get(streamDetails).should("exist");
  }

  close() {
    cy.get(closeButton).click();
  }

  isClosed() {
    cy.get(streamDetails).should("not.exist");
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

  isSyncMode(sourceSyncMode: SourceSyncMode, destSyncMode: DestinationSyncMode) {
    cy.get(syncMode).should(
      "contain.text",
      `${SYNC_MODE_STRINGS[sourceSyncMode]} | ${SYNC_MODE_STRINGS[destSyncMode]}`
    );
  }

  areFieldsValid(fieldNames: string[], fieldDataTypes: string[]) {
    cy.get(streamSourceFieldName).each(($span, i) => {
      expect($span.text()).to.equal(fieldNames[i]);
    });

    cy.get(streamSourceDataType).each(($span, i) => {
      expect($span.text()).to.equal(fieldDataTypes[i]);
    });
  }

  // canScroll() {}

  // selectCursor(fieldName: string) {}

  // isCursorSelected(fieldName: string) {}

  // selectPrimaryKeys(fieldNames: string[]) {}

  // isSourceDefinedCursor() {}

  // isSourceDefinedPrimaryKey() {}
}

export default new StreamDetailsPageObject();
