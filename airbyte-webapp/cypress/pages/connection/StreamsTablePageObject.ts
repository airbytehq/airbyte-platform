import { StreamRowPageObject } from "./StreamRowPageObject";

const refreshSourceSchemaButton = "button[data-testid='refresh-source-schema-btn']";
const streamNameInput = "input[data-testid='input']";

export class StreamsTablePageObject {
  refreshSourceSchemaBtnClick() {
    cy.get(refreshSourceSchemaButton).click();
  }

  searchStream(value: string) {
    cy.get(streamNameInput).clear();
    cy.get(streamNameInput).type(value);
  }

  clearStreamSearch() {
    cy.get(streamNameInput).clear();
  }

  getRow(namespace: string, streamName: string): StreamRowPageObject {
    return new StreamRowPageObject(namespace, streamName);
  }
}

export const streamsTable = new StreamsTablePageObject();
