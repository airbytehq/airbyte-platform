import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";

export const SYNC_MODE_STRINGS: Readonly<Record<SourceSyncMode | DestinationSyncMode, string>> = {
  [SourceSyncMode.FullRefresh]: "Full refresh",
  [SourceSyncMode.Incremental]: "Incremental",
  [DestinationSyncMode.Append]: "Append",
  [DestinationSyncMode.AppendDedup]: "Deduped + history",
  [DestinationSyncMode.Overwrite]: "Overwrite",
};

export interface IStreamsTablePageObject {
  showStreamDetails(namespace: string, streamName: string): void;
  selectSyncMode(source: SourceSyncMode, dest: DestinationSyncMode): void;
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
