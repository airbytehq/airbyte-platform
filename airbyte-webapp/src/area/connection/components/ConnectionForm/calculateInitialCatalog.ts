import isEqual from "lodash/isEqual";

import {
  StreamTransform,
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  SchemaChange,
} from "core/api/types/AirbyteClient";

import { isSameSyncStream } from "./utils";

const clearBreakingFieldChanges = (
  nodeStream: AirbyteStreamAndConfiguration,
  breakingChangesByStream: StreamTransform[]
) => {
  if (!breakingChangesByStream.length || !nodeStream.config) {
    return nodeStream;
  }

  const { primaryKey, cursorField } = nodeStream.config;
  const stream = nodeStream.stream;

  let clearPrimaryKey = false;
  let clearCursorField = false;
  for (const streamTransformation of breakingChangesByStream) {
    if (!streamTransformation.updateStream || !streamTransformation.updateStream?.fieldTransforms?.length) {
      continue;
    }
    // get all of the removed field paths for this transformation
    const breakingFieldPaths = streamTransformation.updateStream.fieldTransforms
      .filter(({ breaking }) => breaking)
      .map((update) => update.fieldName);
    // if there is a primary key in the config, and any of its field paths were removed, we'll be clearing it
    if (
      !!primaryKey?.length &&
      primaryKey?.some((primaryKeyPath) => breakingFieldPaths.some((path) => isEqual(primaryKeyPath, path)))
    ) {
      clearPrimaryKey = true;
    }
    // if there is a cursor field, and any of its field path was removed, we'll be clearing it
    if (!!cursorField?.length && breakingFieldPaths.some((path) => isEqual(path, cursorField))) {
      clearCursorField = true;
    }
  }
  if (clearPrimaryKey || clearCursorField) {
    return {
      ...nodeStream,
      config: {
        ...nodeStream.config,
        primaryKey: stream?.sourceDefinedPrimaryKey // it's possible there's a new source-defined primary key, in which case that should take precedence
          ? stream?.sourceDefinedPrimaryKey
          : clearPrimaryKey
          ? []
          : nodeStream.config.primaryKey,
        cursorField: nodeStream.stream?.defaultCursorField
          ? nodeStream.stream?.defaultCursorField // likewise, a source-defined cursor should never be cleared
          : clearCursorField
          ? []
          : nodeStream.config.cursorField,
      },
    };
  }

  return nodeStream;
};

/**
 * Analyzes the sync catalog for breaking changes and applies necessary transformations.
 * If there are no schema changes or non-breaking changes, and CatalogDiff is undefined, returns the sync catalog unchanged.
 * Otherwise, collects all streams with breaking changes and applies transformations.
 *
 * @param {AirbyteCatalog} syncCatalog - The sync catalog to analyze.
 * @param {CatalogDiff} catalogDiff - The catalog difference.
 * @param {SchemaChange} schemaChange - The schema change type.
 * @returns {AirbyteCatalog} - The modified sync catalog with necessary transformations applied.
 */
export const analyzeSyncCatalogBreakingChanges = (
  syncCatalog: AirbyteCatalog,
  catalogDiff: CatalogDiff | undefined,
  schemaChange: SchemaChange | undefined
): AirbyteCatalog => {
  //  if there are no schema changes or schema change is non-breaking, and CatalogDiff is undefined, return syncCatalog
  if ((SchemaChange.no_change === schemaChange || SchemaChange.non_breaking === schemaChange) && !catalogDiff) {
    return syncCatalog;
  }

  // otherwise, we assume that there is a breaking change and we need to collect all streams with breaking changes
  const streamTransformsWithBreakingChange =
    catalogDiff?.transforms.filter(
      (streamTransform) =>
        streamTransform.transformType === "update_stream" &&
        streamTransform.updateStream?.fieldTransforms?.filter((fieldTransform) => fieldTransform.breaking)
    ) || [];

  return {
    streams: syncCatalog.streams.map<AirbyteStreamAndConfiguration>((nodeStream) => {
      const breakingChangesByStream = streamTransformsWithBreakingChange.filter(({ streamDescriptor }) =>
        isSameSyncStream(nodeStream, streamDescriptor.name, streamDescriptor.namespace)
      );

      if (!breakingChangesByStream.length) {
        return nodeStream;
      }

      return clearBreakingFieldChanges(nodeStream, breakingChangesByStream);
    }),
  };
};
