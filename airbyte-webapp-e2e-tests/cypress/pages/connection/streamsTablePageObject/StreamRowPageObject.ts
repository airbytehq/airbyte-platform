const streamTableRow = (namespace: string, streamName: string) =>
  `div[data-testid='catalog-tree-table-row-${namespace}-${streamName}']`;
const streamPanel = `div[data-testid='stream-details']`;
const streamPanelCloseButton = `button[data-testid='stream-details-close-button']`;
const streamSyncSwitch = `input[data-testid='selected-switch']`;
const sourceStreamNameCell = `div[data-testid='source-stream-name-cell']`;
const destinationStreamNameCell = `div[data-testid='destination-stream-name-cell']`;
const sourceNamespaceCell = `div[data-testid='source-namespace-cell']`;
const destinationNamespaceCell = `div[data-testid='destination-namespace-cell']`;

export class StreamRowPageObject {
  stream: string;
  namespace: string;
  streamName: string;

  constructor(namespace: string, streamName: string) {
    this.namespace = namespace;
    this.streamName = streamName;
    this.stream = streamTableRow(namespace, streamName);

    // bind for destructuring
    this.isStreamSyncEnabled = this.isStreamSyncEnabled.bind(this);
    this.toggleStreamSync = this.toggleStreamSync.bind(this);
    this.isStreamRowHasRemovedStyle = this.isStreamRowHasRemovedStyle.bind(this);
    this.checkSourceNamespace = this.checkSourceNamespace.bind(this);
    this.checkSourceStreamName = this.checkSourceStreamName.bind(this);
    this.checkDestinationNamespace = this.checkDestinationNamespace.bind(this);
    this.checkDestinationStreamName = this.checkDestinationStreamName.bind(this);
    this.openStreamPanel = this.openStreamPanel.bind(this);
    this.closeStreamPanel = this.closeStreamPanel.bind(this);
    this.isStreamPanelVisible = this.isStreamPanelVisible.bind(this);
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

  isStreamRowHasRemovedStyle(expectedValue: boolean) {
    cy.get(this.stream)
      .invoke("attr", "class")
      .should(`${expectedValue ? "" : "not."}match`, /removed/);
  }

  checkSourceNamespace() {
    cy.get(this.stream).within(() => cy.get(sourceNamespaceCell).contains(this.namespace));
  }

  checkSourceStreamName() {
    cy.get(this.stream).within(() => cy.get(sourceStreamNameCell).contains(this.streamName));
  }

  checkDestinationNamespace(expectedValue: string) {
    cy.get(this.stream).within(() => cy.get(destinationNamespaceCell).contains(expectedValue));
  }
  checkDestinationStreamName(expectedValue: string) {
    cy.get(this.stream).within(() => cy.get(destinationStreamNameCell).contains(expectedValue));
  }

  openStreamPanel() {
    cy.get(this.stream).within(() => cy.get(destinationNamespaceCell).click());
  }

  closeStreamPanel() {
    cy.get(streamPanel).within(() => cy.get(streamPanelCloseButton).click());
  }

  isStreamPanelVisible(expectedValue: boolean) {
    cy.get(streamPanel).should(`${expectedValue ? "" : "not."}exist`);
  }
}
