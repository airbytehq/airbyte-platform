export interface IStreamsTablePageObject {
  showStreamDetails(namespace: string, streamName: string): void;
  selectSyncMode(source: string, dest: string): void;
  selectCursor(streamName: string, cursorValue: string): void;
  selectPrimaryKeys(streamName: string, primaryKeyValues: string[]): void;
  checkStreamFields(listNames: string[], listTypes: string[]): void;
  checkSelectedCursorField(streamName: string, expectedValue: string): void;
  checkSourceDefinedCursor(streamName: string, expectedValue: string): void;
  checkSelectedPrimaryKeys(streamName: string, expectedValues: string[]): void;
  checkSourceDefinedPrimaryKeys(streamName: string, expectedValue: string): void;
  hasEmptyCursorSelect(namespace: string, streamName: string): void;
  hasEmptyPrimaryKeySelect(namespace: string, streamName: string): void;
  checkNoSourceDefinedCursor(namespace: string, streamName: string): void;
  checkNoSourceDefinedPrimaryKeys(namespace: string, streamName: string): void;
  enableStream(namespace: string, streamName: string): void;
  disableStream(namespace: string, streamName: string): void;
}
