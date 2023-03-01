import { IStreamsTablePageObject, SYNC_MODE_STRINGS } from "./types";
import { StreamsTablePageObjectBase } from "./StreamsTableContainerPageObject";
import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";

const syncModeDropdown = "div[data-testid='syncSettingsDropdown'] input";
const getFieldDropdownContainer = (streamName: string, type: Dropdown) => `div[id='${streamName}_${type}_pathPopout']`;
const getFieldDropdownButton = (streamName: string, type: Dropdown) =>
  `button[data-testid='${streamName}_${type}_pathPopout']`;
const getFieldDropdownOption = (value: string) => `div[data-testid='${value}']`;
const dropDownOverlayContainer = "div[data-testid='overlayContainer']";
const streamNameCell = "[data-testid='nameCell']";
const streamDataTypeCell = "[data-testid='dataTypeCell']";
const getExpandStreamArrowBtn = (streamName: string) => `[data-testid='${streamName}_expandStreamDetails']`;
const getPreFilledPrimaryKeyText = (streamName: string) => `[data-testid='${streamName}_primaryKey_pathPopout_text']`;
const streamSyncEnabledSwitch = (streamName: string) => `[data-testid='${streamName}-stream-sync-switch']`;

type Dropdown = "cursor" | "primaryKey";

/**
 * General function - select dropdown option(s)
 * @param streamName
 * @param dropdownType
 * @param value
 */
const selectFieldDropdownOption = (streamName: string, dropdownType: Dropdown, value: string | string[]) => {
  const container = getFieldDropdownContainer(streamName, dropdownType);
  const button = getFieldDropdownButton(streamName, dropdownType);

  cy.get(container).within(() => {
    cy.get(button).click();

    if (Array.isArray(value)) {
      // in case if multiple options need to be selected
      value.forEach((v) => cy.get(getFieldDropdownOption(v)).click());
    } else {
      // in case if one option need to be selected
      cy.get(getFieldDropdownOption(value)).click();
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

/**
 * General function - check selected field dropdown option or options
 * @param streamName
 * @param dropdownType
 * @param expectedValue
 */
const checkDropdownField = (streamName: string, dropdownType: Dropdown, expectedValue: string | string[]) => {
  const button = getFieldDropdownButton(streamName, dropdownType);
  const isButtonContainsExactValue = (value: string) => cy.get(button).contains(new RegExp(`^${value}$`));

  return Array.isArray(expectedValue)
    ? expectedValue.every((value) => isButtonContainsExactValue(value))
    : isButtonContainsExactValue(expectedValue);
};

export class LegacyStreamsTablePageObject extends StreamsTablePageObjectBase implements IStreamsTablePageObject {
  showStreamDetails(namespace: string, streamName: string) {
    cy.get(getExpandStreamArrowBtn(streamName)).click();
  }

  selectSyncMode(source: SourceSyncMode, dest: DestinationSyncMode): void {
    cy.get(syncModeDropdown).first().click({ force: true });

    cy.get(`.react-select__option`)
      .contains(`Source:${SYNC_MODE_STRINGS[source]}|Dest:${SYNC_MODE_STRINGS[dest]}`)
      .click();
  }

  /**
   * Select cursor value from cursor dropdown(pathPopout) in desired stream
   * @param streamName
   * @param cursorValue
   */
  selectCursor(streamName: string, cursorValue: string) {
    selectFieldDropdownOption(streamName, "cursor", cursorValue);
  }

  /**
   * Select primary key value(s) from primary key dropdown(pathPopout) in desired stream
   * @param streamName
   * @param primaryKeyValues
   */
  selectPrimaryKeys(streamName: string, primaryKeyValues: string[]) {
    selectFieldDropdownOption(streamName, "primaryKey", primaryKeyValues);
  }

  checkStreamFields(listNames: string[], listTypes: string[]) {
    cy.get(streamNameCell).each(($span, i) => {
      expect($span.text()).to.equal(listNames[i]);
    });

    cy.get(streamDataTypeCell).each(($span, i) => {
      expect($span.text()).to.equal(listTypes[i]);
    });
  }

  /**
   * Check selected value in cursor dropdown
   * @param streamName
   * @param expectedValue
   */
  checkSelectedCursorField(streamName: string, expectedValue: string) {
    checkDropdownField(streamName, "cursor", expectedValue);
  }

  /**
   * Check selected value(s) in primary key dropdown
   * @param streamName
   * @param expectedValues
   */
  checkSelectedPrimaryKeys(streamName: string, expectedValues: string[]) {
    checkDropdownField(streamName, "primaryKey", expectedValues);
  }

  checkSourceDefinedPrimaryKeys(streamName: string, expectedValue: string) {
    cy.get(getPreFilledPrimaryKeyText(streamName)).contains(expectedValue);
  }

  checkNoSourceDefinedPrimaryKeys(namespace: string, streamName: string) {
    cy.get(getPreFilledPrimaryKeyText(streamName)).should("not.exist");
  }

  enableStream(namespace: string, streamName: string) {
    cy.get(streamSyncEnabledSwitch(streamName)).check({ force: true });
  }

  checkSourceDefinedCursor(streamName: string, expectedValue: string): void {
    throw new Error("Method not implemented.");
  }

  hasEmptyCursorSelect(namespace: string, streamName: string): void {
    throw new Error("Method not implemented.");
  }

  hasEmptyPrimaryKeySelect(namespace: string, streamName: string): void {
    throw new Error("Method not implemented.");
  }

  checkNoSourceDefinedCursor(namespace: string, streamName: string): void {
    throw new Error("Method not implemented.");
  }

  disableStream(namespace: string, streamName: string): void {
    throw new Error("Method not implemented.");
  }
}
