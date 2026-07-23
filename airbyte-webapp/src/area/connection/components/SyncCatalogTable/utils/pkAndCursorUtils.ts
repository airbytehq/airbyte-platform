import isEqual from "lodash/isEqual";

import { AirbyteStream, AirbyteStreamConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

type Path = string[];
type FieldPathType = null | "required" | "sourceDefined";

export function isCursor(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return config ? isEqual(config?.cursorField, path) : false;
}

export function isChildFieldCursor(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return config?.cursorField ? isEqual([config.cursorField[0]], path) : false;
}

export function isPrimaryKey(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return !!config?.primaryKey?.some((p) => isEqual(p, path));
}

export function isChildFieldPrimaryKey(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return !!config?.primaryKey?.some((p) => isEqual([p[0]], path));
}

export const getFieldPathType = (required: boolean, shouldDefine: boolean): FieldPathType =>
  required ? (shouldDefine ? "required" : "sourceDefined") : null;

export const getFieldPathDisplayName = (path: Path): string => path.join(".");

/**
 * Checks if the stream has a required cursor or primary key and if the user has to defined it
 * @param config
 * @param stream
 */
export const checkCursorAndPKRequirements = (config: AirbyteStreamConfiguration, stream: AirbyteStream) => {
  const pkRequired =
    config?.destinationSyncMode === DestinationSyncMode.append_dedup ||
    config?.destinationSyncMode === DestinationSyncMode.overwrite_dedup;
  const cursorRequired = config?.syncMode === SyncMode.incremental;
  const shouldDefinePk = stream?.sourceDefinedPrimaryKey?.length === 0 && pkRequired;
  const shouldDefineCursor = !stream?.sourceDefinedCursor && cursorRequired;

  return { pkRequired, cursorRequired, shouldDefinePk, shouldDefineCursor };
};

/**
 * Check if a field is a CDC meta-field (starts with _ab_cdc_)
 */
export function isCdcMetaField(fieldPath: string[]): boolean {
  return fieldPath.length > 0 && fieldPath[0].startsWith("_ab_cdc_");
}
