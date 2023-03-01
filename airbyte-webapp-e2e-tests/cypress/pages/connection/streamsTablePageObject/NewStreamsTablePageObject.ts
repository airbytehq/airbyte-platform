import { getTestId, joinTestIds } from "utils/selectors";
import { IStreamsTablePageObject, SYNC_MODE_STRINGS } from "./types";
import { StreamsTablePageObjectBase } from "./StreamsTableContainerPageObject";
import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";

const syncModeSelectButton = joinTestIds(getTestId("sync-mode-select"), getTestId("pill-select-button"));

type SyncFieldType = "cursor" | "primary-key";

const getRowTestId = (namespace: string, streamName: string, childTestId?: string) =>
  joinTestIds(getTestId(`catalog-tree-table-row-${namespace}-${streamName}`), childTestId);

const getFieldSelectButtonTestId = (streamName: string, type: SyncFieldType) =>
  joinTestIds(getTestId(`${type}-select`), getTestId("pill-select-button"));
const getSourceDefinedTestId = (type: SyncFieldType) => getTestId(`${type}-text`);
const getStreamSwitchTestId = (namespace: string, streamName: string) =>
  getRowTestId(namespace, streamName, getTestId("sync-switch"));
const dropDownOverlayContainer = getTestId("overlayContainer");

const streamSourceFieldName = getTestId("stream-source-field-name");
const streamSourceDataType = getTestId("stream-source-field-data-type");

const selectFieldOption = (streamName: string, dropdownType: SyncFieldType, value: string | string[]) => {
  const container = getRowTestId("public", streamName);
  const button = getFieldSelectButtonTestId(streamName, dropdownType);

  cy.get(container).within(() => {
    cy.get(button).click();

    if (Array.isArray(value)) {
      // in case if multiple options need to be selected
      value.forEach((v) => cy.get(getTestId(v)).click());
    } else {
      // in case if one option need to be selected
      cy.get(getTestId(value)).click();
    }
  });

  // close dropdown
  // (dropdown need to be closed manually by clicking on overlay in case if multiple option selection is available)
  cy.get("body").then(($body) => {
    if ($body.find(dropDownOverlayContainer).length > 0) {
      cy.get(dropDownOverlayContainer).click();
    }
  });
};

const checkFieldSelectedValue = (streamName: string, type: SyncFieldType, expectedValue: string | string[]) => {
  const container = getRowTestId("public", streamName);
  const button = getFieldSelectButtonTestId(streamName, type);
  const expected = Array.isArray(expectedValue) ? expectedValue.join(", ") : expectedValue;

  cy.get(container).within(() => {
    cy.get(button).contains(new RegExp(`^${expected}$`));
  });
};

export class NewStreamsTablePageObject extends StreamsTablePageObjectBase implements IStreamsTablePageObject {
  showStreamDetails(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).click(1, 1);
  }

  selectSyncMode(source: SourceSyncMode, dest: DestinationSyncMode): void {
    cy.get(syncModeSelectButton).first().click({ force: true });
    cy.get(`.react-select__option`).contains(`${SYNC_MODE_STRINGS[source]} | ${SYNC_MODE_STRINGS[dest]}`).click();
  }

  selectCursor(streamName: string, cursorValue: string): void {
    selectFieldOption(streamName, "cursor", cursorValue);
  }

  selectPrimaryKeys(streamName: string, primaryKeyValues: string[]): void {
    selectFieldOption(streamName, "primary-key", primaryKeyValues);
  }

  checkStreamFields(listNames: string[], listTypes: string[]): void {
    cy.get(streamSourceFieldName).each(($span, i) => {
      expect($span.text()).to.equal(listNames[i]);
    });

    cy.get(streamSourceDataType).each(($span, i) => {
      expect($span.text()).to.equal(listTypes[i]);
    });
  }

  checkSelectedCursorField(streamName: string, expectedValue: string): void {
    checkFieldSelectedValue(streamName, "cursor", expectedValue);
  }

  checkSelectedPrimaryKeys(streamName: string, expectedValues: string[]): void {
    checkFieldSelectedValue(streamName, "primary-key", expectedValues);
  }

  checkSourceDefinedPrimaryKeys(streamName: string, expectedValue: string): void {
    cy.get(getRowTestId("public", streamName)).within(() => {
      cy.get(getSourceDefinedTestId("primary-key")).contains(expectedValue);
    });
  }

  checkSourceDefinedCursor(streamName: string, expectedValue: string): void {
    cy.get(getRowTestId("public", streamName)).within(() => {
      cy.get(getSourceDefinedTestId("cursor")).contains(expectedValue);
    });
  }

  hasEmptyCursorSelect(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      const button = getFieldSelectButtonTestId(streamName, "cursor");
      cy.get(button).should("have.attr", "data-error", "true").should("not.contain.text");
    });
  }

  hasEmptyPrimaryKeySelect(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      const button = getFieldSelectButtonTestId(streamName, "primary-key");
      cy.get(button).should("have.attr", "data-error", "true").should("not.contain.text");
    });
  }

  checkNoSourceDefinedCursor(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      cy.get(getSourceDefinedTestId("cursor")).should("not.exist");
    });
  }

  checkNoSourceDefinedPrimaryKeys(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      cy.get(getSourceDefinedTestId("primary-key")).should("not.exist");
    });
  }

  enableStream(namespace: string, streamName: string): void {
    cy.get(getStreamSwitchTestId(namespace, streamName)).check({ force: true });
  }

  disableStream(namespace: string, streamName: string): void {
    cy.get(getStreamSwitchTestId(namespace, streamName)).uncheck({ force: true });
  }
}
