const streamTableRow = (namespace: string, streamName: string) =>
  `div[data-testid='catalog-tree-table-row-${namespace}-${streamName}']`;
const streamPanel = `div[data-testid='stream-details']`;
const streamPanelCloseButton = `button[data-testid='stream-details-close-button']`;
const streamSyncSwitch = `input[data-testid='sync-switch']`;
const sourceStreamNameCell = `div[data-testid='source-stream-name-cell']`;
const destinationStreamNameCell = `div[data-testid='destination-stream-name-cell']`;
const sourceNamespaceCell = `div[data-testid='source-namespace-cell']`;
const destinationNamespaceCell = `div[data-testid='destination-namespace-cell']`;

export class StreamRowPageObject {
  static getStreamUtilityFunctions(namespace: string, streamName: string) {
    const stream = streamTableRow(namespace, streamName);

    const isStreamSyncEnabled = (expectedValue: boolean) =>
      cy.get(stream).within(() => {
        cy.get(streamSyncSwitch).should(`${expectedValue ? "" : "not."}be.checked`);
      });

    const toggleStreamSync = () =>
      cy.get(stream).within(() => {
        cy.get(streamSyncSwitch).closest("label").click();
      });

    const isStreamRowHasRemovedStyle = (expectedValue: boolean) =>
      cy
        .get(stream)
        .invoke("attr", "class")
        .should(`${expectedValue ? "" : "not."}match`, /removed/);

    const checkSourceNamespace = () => cy.get(stream).within(() => cy.get(sourceNamespaceCell).contains(namespace));
    const checkSourceStreamName = () => cy.get(stream).within(() => cy.get(sourceStreamNameCell).contains(streamName));

    const checkDestinationNamespace = (expectedValue: string) =>
      cy.get(stream).within(() => cy.get(destinationNamespaceCell).contains(expectedValue));
    const checkDestinationStreamName = (expectedValue: string) =>
      cy.get(stream).within(() => cy.get(destinationStreamNameCell).contains(expectedValue));

    const openStreamPanel = () => cy.get(stream).within(() => cy.get(destinationNamespaceCell).click());
    const closeStreamPanel = () => cy.get(streamPanel).within(() => cy.get(streamPanelCloseButton).click());
    const isStreamPanelVisible = (expectedValue: boolean) =>
      cy.get(streamPanel).should(`${expectedValue ? "" : "not."}exist`);

    return {
      isStreamSyncEnabled,
      toggleStreamSync,
      isStreamRowHasRemovedStyle,
      checkSourceNamespace,
      checkSourceStreamName,
      checkDestinationNamespace,
      checkDestinationStreamName,
      openStreamPanel,
      closeStreamPanel,
      isStreamPanelVisible,
    };
  }
}
