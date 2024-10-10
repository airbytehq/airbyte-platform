import { SYNC_MODE_STRINGS } from "@cy/pages/connection/types";
import { getTestId } from "@cy/utils/selectors";
import { DestinationSyncMode, SyncMode } from "@src/core/api/generated/AirbyteClient.schemas";

export const getNamespaceRowTestId = (namespace: string) => getTestId(`row-depth-0-namespace-${namespace}`, "tr");
const getStreamRowTestId = (streamName: string) => getTestId(`row-depth-1-stream-${streamName}`, "tr");
const getFieldRowTestId = (fieldName: string) => getTestId(`row-depth-2-field-${fieldName}`, "tr");

const streamSyncCheckbox = getTestId("sync-stream-checkbox", "input");
const streamExpandCollapseButton = getTestId("expand-collapse-stream-btn", "button");
const streamSyncModeSelectButton = getTestId("sync-mode-select-listbox-button", "button");

const fieldSyncCheckbox = getTestId("sync-field-checkbox", "input");
const fieldPKCell = getTestId("field-pk-cell", "div");
const fieldCursorCell = getTestId("field-cursor-cell", "div");

export class StreamRowPageObjectV2 {
  private readonly namespace: string;
  private readonly stream: string;

  constructor(namespace: string, streamName: string) {
    this.namespace = getNamespaceRowTestId(namespace);
    this.stream = getStreamRowTestId(streamName);
  }

  private withinStream(callback: () => void) {
    cy.get(this.namespace).nextAll('[data-testid^="row-depth-1-stream"]').filter(this.stream).first().within(callback);
  }

  private withinField(fieldName: string, callback: () => void) {
    cy.get(this.namespace)
      .nextAll('[data-testid^="row-depth-1-stream"]')
      .filter(this.stream)
      .first()
      .nextAll('[data-testid^="row-depth-2-field"]')
      .filter(getFieldRowTestId(fieldName))
      .first()
      .within(callback);
  }

  // Stream sync
  toggleStreamSync(enabled: boolean) {
    this.withinStream(() => {
      if (enabled) {
        cy.get(streamSyncCheckbox).check({ force: true });
      } else {
        cy.get(streamSyncCheckbox).uncheck({ force: true });
      }
    });
  }

  isStreamSyncEnabled(expectedResult: boolean) {
    this.withinStream(() => {
      cy.get(streamSyncCheckbox).should(expectedResult ? "be.checked" : "not.be.checked");
    });
  }

  // Expand/collapse stream
  toggleExpandCollapseStream() {
    this.withinStream(() => {
      cy.get(streamExpandCollapseButton).click();
    });
  }

  isStreamExpanded(expectedValue: boolean) {
    this.withinStream(() => {
      cy.get(streamExpandCollapseButton).should("have.attr", "aria-expanded", expectedValue.toString());
    });
  }

  // Sync mode
  selectSyncMode(source: SyncMode, dest: DestinationSyncMode): void {
    const syncMode = `${SYNC_MODE_STRINGS[source]} | ${SYNC_MODE_STRINGS[dest]}`;

    this.withinStream(() => {
      cy.get(streamSyncModeSelectButton).click({ force: true });
      cy.get('li[role="option"]')
        // It's possible that there are multiple options with the same text, so we need to filter by exact text content
        // instead of using .contains(), e.g. "Incremental | Append" and "Incremental | Append + Dedupe"
        .filter((_, element) => Cypress.$(element).text().trim() === syncMode)
        .should("have.length", 1)
        .click({ force: true });
    });
  }

  // PK
  isMissedPKErrorDisplayed(expectedResult: boolean) {
    this.withinStream(() => {
      cy.contains("Primary key missing").should(expectedResult ? "be.visible" : "not.be.visible");
    });
  }

  // Cursor
  isMissedCursorErrorDisplayed(expectedResult: boolean) {
    this.withinStream(() => {
      cy.contains("Cursor missing").should(expectedResult ? "be.visible" : "not.be.visible");
    });
  }

  // Fields
  toggleFieldSync(fieldName: string, enabled: boolean) {
    this.withinField(fieldName, () => {
      if (enabled) {
        cy.get(fieldSyncCheckbox).check({ force: true });
      } else {
        cy.get(fieldSyncCheckbox).uncheck({ force: true });
      }
    });
  }

  isFieldSyncEnabled(fieldName: string, expectedResult: boolean) {
    this.withinField(fieldName, () => {
      cy.get(fieldSyncCheckbox).should(expectedResult ? "be.checked" : "not.be.checked");
    });
  }

  isFieldSyncCheckboxDisabled(fieldName: string, expectedResult: boolean) {
    this.withinField(fieldName, () => {
      cy.get(fieldSyncCheckbox).should(expectedResult ? "be.disabled" : "not.be.disabled");
    });
  }

  isPKField(fieldName: string, expectedResult: boolean) {
    this.withinField(fieldName, () => {
      cy.get(fieldPKCell).should(`${expectedResult ? "" : "not."}contain`, "primary key");
    });
  }

  isCursorField(fieldName: string, expectedResult: boolean) {
    this.withinField(fieldName, () => {
      cy.get(fieldCursorCell).should(`${expectedResult ? "" : "not."}contain`, "cursor");
    });
  }

  toggleAllFieldsSync(enabled: boolean) {
    // TODO: implement
  }

  areAllFieldsEnabled() {
    // TODO: implement
  }

  // Stream row styles
  private checkStreamStyle(className: string, expectedResult: boolean) {
    cy.get(this.namespace)
      .siblings(this.stream)
      .invoke("attr", "class")
      .should(`${expectedResult ? "" : "not."}match`, new RegExp(className));
  }

  streamHasChangedStyle(expectedResult: boolean) {
    this.checkStreamStyle("changed", expectedResult);
  }

  streamHasRemovedStyle(expectedResult: boolean) {
    this.checkStreamStyle("removed", expectedResult);
  }

  streamHasAddedStyle(expectedValue: boolean) {
    this.checkStreamStyle("added", expectedValue);
  }

  streamHasDisabledStyle(expectedValue: boolean) {
    this.checkStreamStyle("disabled", expectedValue);
  }

  // Field row styles
  private checkFieldStyle(fieldName: string, className: string, expectedResult: boolean) {
    cy.get(this.namespace)
      .siblings(this.stream)
      .siblings(getFieldRowTestId(fieldName))
      .invoke("attr", "class")
      .should(`${expectedResult ? "" : "not."}match`, new RegExp(className));
  }

  fieldHasRemovedStyle(fieldName: string, expectedResult: boolean) {
    this.checkFieldStyle(fieldName, "removed", expectedResult);
  }

  fieldHasAddedStyle(fieldName: string, expectedResult: boolean) {
    this.checkFieldStyle(fieldName, "added", expectedResult);
  }

  fieldHasDisabledStyle(fieldName: string, expectedResult: boolean) {
    this.checkFieldStyle(fieldName, "disabled", expectedResult);
  }
}
