import { getTestId, joinTestIds } from "utils/selectors";
import { IStreamsTablePageObject } from "./IStreamsTablePageObject";
import { StreamsTablePageObjectBase } from "./StreamsTableContainerPageObject";

const syncModeSelectButton = joinTestIds(getTestId("sync-mode-select"), getTestId("pill-select-button"));

type SyncFieldType = "cursor" | "primary-key";

const getRowTestId = (namespace: string, streamName: string, childTestId?: string) =>
  joinTestIds(getTestId(`catalog-tree-table-row-${namespace}-${streamName}`), childTestId);

const getFieldSelectButtonTestId = (streamName: string, type: SyncFieldType) =>
  joinTestIds(getTestId(`${type}-select`), getTestId("pill-select-button"));
const getSourceDefinedTestId = (type: SyncFieldType) => getTestId(`${type}-text`);
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

const checkFieldSelectedValue = (streamName: string, dropdownType: SyncFieldType, expectedValue: string | string[]) => {
  const container = getRowTestId("public", streamName);
  const button = getFieldSelectButtonTestId(streamName, dropdownType);
  const isButtonContainsExactValue = (value: string) => cy.get(button).contains(new RegExp(`^${value}$`));

  cy.get(container).within(() => {
    Array.isArray(expectedValue)
      ? expectedValue.every((value) => isButtonContainsExactValue(value))
      : isButtonContainsExactValue(expectedValue);
  });
};

export class NewStreamsTablePageObject extends StreamsTablePageObjectBase implements IStreamsTablePageObject {
  expandStreamDetailsByName(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).click(1, 1);
  }

  selectSyncMode(source: string, dest: string): void {
    cy.get(syncModeSelectButton).first().click({ force: true });
    cy.get(`.react-select__option`).contains(`${source} | ${dest}`).click();
  }

  selectCursorField(streamName: string, cursorValue: string): void {
    selectFieldOption(streamName, "cursor", cursorValue);
  }

  selectPrimaryKeyField(streamName: string, primaryKeyValues: string[]): void {
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

  checkCursorField(streamName: string, expectedValue: string): void {
    checkFieldSelectedValue(streamName, "cursor", expectedValue);
  }

  checkPrimaryKey(streamName: string, expectedValues: string[]): void {
    checkFieldSelectedValue(streamName, "primary-key", expectedValues);
  }

  checkPreFilledPrimaryKeyField(streamName: string, expectedValue: string): void {
    cy.get(getRowTestId("public", streamName)).within(() => {
      cy.get(getSourceDefinedTestId("primary-key")).contains(expectedValue);
    });
  }

  checkPreFilledCursorField(streamName: string, expectedValue: string): void {
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

  isCursorNonExist(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      cy.get(getSourceDefinedTestId("cursor")).should("not.exist");
    });
  }

  isPrimaryKeyNonExist(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName)).within(() => {
      cy.get(getSourceDefinedTestId("primary-key")).should("not.exist");
    });
  }

  toggleStreamEnabledState(namespace: string, streamName: string): void {
    cy.get(getRowTestId(namespace, streamName, getTestId("selected-switch"))).check({ force: true });
  }
}
