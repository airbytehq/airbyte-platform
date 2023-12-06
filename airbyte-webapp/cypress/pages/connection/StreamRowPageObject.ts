import { DestinationSyncMode, SyncMode } from "@src/core/api/types/AirbyteClient";
import { getTestId, getTestIds, joinTestIds } from "utils/selectors";

import { SYNC_MODE_STRINGS } from "./types";

type SyncFieldType = "cursor" | "primary-key";

const streamTableRow = (namespace: string, streamName: string) =>
  getTestId(`catalog-tree-table-row-${namespace}-${streamName}`, "div");

const getFieldSelectButtonTestId = (type: SyncFieldType) =>
  joinTestIds(getTestId(`${type}-select`), getTestId("pill-select-button"));

const getSourceDefinedTestId = (type: SyncFieldType) => getTestId(`${type}-text`);

export const syncModeSelectButton = getTestId("sync-mode-select-listbox-button");

const [streamSyncSwitch, destinationStreamNameCell, destinationNamespaceCell, dropDownOverlayContainer] = getTestIds(
  ["selected-switch", "input"],
  ["destination-stream-name-cell", "div"],
  ["destination-namespace-cell", "div"],
  "overlayContainer"
);

export class StreamRowPageObject {
  private readonly stream: string;

  constructor(
    private readonly namespace: string,
    private readonly streamName: string
  ) {
    this.stream = streamTableRow(namespace, streamName);
  }

  private selectFieldOption(dropdownType: SyncFieldType, value: string | string[]) {
    cy.get(this.stream).within(() => {
      const button = getFieldSelectButtonTestId(dropdownType);
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
  }

  private checkFieldSelectedValue(type: SyncFieldType, expectedValue: string | string[]) {
    cy.get(this.stream).within(() => {
      const button = getFieldSelectButtonTestId(type);
      const expected = Array.isArray(expectedValue) ? expectedValue.join(", ") : expectedValue;
      cy.get(button).contains(new RegExp(`^${expected}$`));
    });
  }

  isStreamSyncEnabled(expectedValue: boolean) {
    cy.get(this.stream).within(() => {
      cy.get(streamSyncSwitch).should(`${expectedValue ? "" : "not."}be.checked`);
    });
  }

  toggleStreamSync() {
    cy.get(this.stream).within(() => {
      cy.get(streamSyncSwitch).parent().click();
    });
  }

  checkSyncToggleDisabled() {
    cy.get(this.stream).within(() => {
      cy.get(streamSyncSwitch).should("be.disabled");
    });
  }

  hasRemovedStyle(expectedValue: boolean) {
    cy.get(this.stream)
      .invoke("attr", "class")
      .should(`${expectedValue ? "" : "not."}match`, /removed/);
  }

  hasAddedStyle(expectedValue: boolean) {
    cy.get(this.stream)
      .invoke("attr", "class")
      .should(`${expectedValue ? "" : "not."}match`, /added/);
  }

  checkDestinationNamespace(expectedValue: string) {
    cy.get(this.stream).within(() => cy.get(destinationNamespaceCell).should("have.text", expectedValue));
  }

  checkDestinationStreamName(expectedValue: string) {
    cy.get(this.stream).within(() => cy.get(destinationStreamNameCell).should("have.text", expectedValue));
  }

  showStreamDetails() {
    cy.get(this.stream).within(() => cy.get(destinationNamespaceCell).click());
  }

  selectSyncMode(source: SyncMode, dest: DestinationSyncMode): void {
    const syncMode = `${SYNC_MODE_STRINGS[source]} | ${SYNC_MODE_STRINGS[dest]}`;
    cy.get(this.stream).scrollIntoView();
    cy.get(this.stream).within(() => {
      cy.get(syncModeSelectButton).click({ force: true });
      cy.get('li[role="option"]')
        // It's possible that there are multiple options with the same text, so we need to filter by exact text content
        // instead of using .contains(), e.g. "Incremental | Append" and "Incremental | Append + Dedupe"
        .filter((_, element) => Cypress.$(element).text().trim() === syncMode)
        .should("have.length", 1)
        .click();
    });
  }

  checkSyncModeDropdownDisabled() {
    cy.get(this.stream).within(() => {
      cy.get(syncModeSelectButton).should("be.disabled");
    });
  }

  hasSelectedSyncMode(source: SyncMode, dest: DestinationSyncMode): void {
    cy.get(this.stream).within(() => {
      cy.get(syncModeSelectButton).contains(`${SYNC_MODE_STRINGS[source]}`);
      cy.get(syncModeSelectButton).contains(`${SYNC_MODE_STRINGS[dest]}`);
    });
  }

  verifyCursor(expectedValue: string): void {
    cy.get(this.stream).within(() => {
      cy.get(getTestId("cursor-field-cell")).contains(expectedValue);
    });
  }

  verifyPrimaryKeys(expectedValues: string[]): void {
    cy.get(this.stream).within(() => {
      cy.get(getTestId("primary-key-cell")).contains(expectedValues.join(", "));
    });
  }

  hasEmptyCursorSelect(): void {
    cy.get(this.stream).within(() => {
      const button = getFieldSelectButtonTestId("cursor");
      cy.get(button).should("have.attr", "data-error", "true").should("not.contain.text");
    });
  }

  hasEmptyPrimaryKeySelect(): void {
    cy.get(this.stream).within(() => {
      const button = getFieldSelectButtonTestId("primary-key");
      cy.get(button).should("have.attr", "data-error", "true").should("not.contain.text");
    });
  }

  hasNoSourceDefinedCursor(): void {
    cy.get(this.stream).within(() => {
      cy.get(getSourceDefinedTestId("cursor")).should("not.exist");
    });
  }

  hasNoSourceDefinedPrimaryKeys(): void {
    cy.get(this.stream).within(() => {
      cy.get(getSourceDefinedTestId("primary-key")).should("not.exist");
    });
  }
}
