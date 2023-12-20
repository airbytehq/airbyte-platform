import isEqual from "lodash/isEqual";

import {
  DestinationSyncMode,
  SyncMode,
  AirbyteStreamConfiguration,
  StreamDescriptor,
  StreamTransform,
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  SchemaChange,
} from "core/api/types/AirbyteClient";

import { isSameSyncStream } from "./utils";

/**
 * Will be removed soon, there is no need to get default cursor field, we get it from the backend
 * @deprecated
 */
const getDefaultCursorField = (streamNode: AirbyteStreamAndConfiguration): string[] => {
  if (streamNode.stream?.defaultCursorField?.length) {
    return streamNode.stream.defaultCursorField;
  }
  return streamNode.config?.cursorField || [];
};

const clearBreakingFieldChanges = (
  nodeStream: AirbyteStreamAndConfiguration,
  breakingChangesByStream: StreamTransform[]
) => {
  if (!breakingChangesByStream.length || !nodeStream.config) {
    return nodeStream;
  }

  const { primaryKey, cursorField } = nodeStream.config;

  let clearPrimaryKey = false;
  let clearCursorField = false;

  for (const streamTransformation of breakingChangesByStream) {
    if (!streamTransformation.updateStream || !streamTransformation.updateStream?.length) {
      continue;
    }

    // get all of the removed field paths for this transformation
    const breakingFieldPaths = streamTransformation.updateStream
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
        primaryKey: clearPrimaryKey ? [] : nodeStream.config.primaryKey,
        cursorField: clearCursorField ? [] : nodeStream.config.cursorField,
      },
    };
  }

  return nodeStream;
};

/**
 * Will be removed soon, there is no need
 * @deprecated
 */
const verifySourceDefinedProperties = (streamNode: AirbyteStreamAndConfiguration, isEditMode: boolean) => {
  if (!streamNode.stream || !streamNode.config || !isEditMode) {
    return streamNode;
  }

  const {
    stream: { sourceDefinedPrimaryKey, sourceDefinedCursor },
  } = streamNode;

  // if there's a source-defined cursor and the mode is correct, set the config to the default
  if (sourceDefinedCursor) {
    streamNode.config.cursorField = streamNode.stream.defaultCursorField;
  }

  // if the primary key doesn't need to be calculated from the source, just return the node
  if (!sourceDefinedPrimaryKey || sourceDefinedPrimaryKey.length === 0) {
    return streamNode;
  }

  // override the primary key with what the source said
  streamNode.config.primaryKey = sourceDefinedPrimaryKey;

  return streamNode;
};

/**
 * Will be removed soon, there is no need to verify supported sync modes, we get it from the backend
 * @deprecated
 */
const verifySupportedSyncModes = (streamNode: AirbyteStreamAndConfiguration): AirbyteStreamAndConfiguration => {
  if (!streamNode.stream) {
    return streamNode;
  }
  const {
    stream: { supportedSyncModes },
  } = streamNode;

  if (supportedSyncModes?.length) {
    return streamNode;
  }
  return { ...streamNode, stream: { ...streamNode.stream, supportedSyncModes: [SyncMode.full_refresh] } };
};

/**
 * Will be removed soon, there is no need to verify cursor field, we get it from the backend
 * @deprecated
 */
const verifyConfigCursorField = (streamNode: AirbyteStreamAndConfiguration): AirbyteStreamAndConfiguration => {
  if (!streamNode.config) {
    return streamNode;
  }
  const { config } = streamNode;

  return {
    ...streamNode,
    config: {
      ...config,
      cursorField: config.cursorField?.length ? config.cursorField : getDefaultCursorField(streamNode),
    },
  };
};

/**
 * Will be removed soon, there is no need to get optimal sync mode, we get it from the backend
 * @deprecated
 */
const getOptimalSyncMode = (
  streamNode: AirbyteStreamAndConfiguration,
  supportedDestinationSyncModes: DestinationSyncMode[]
): AirbyteStreamAndConfiguration => {
  const updateStreamConfig = (
    config: Pick<AirbyteStreamConfiguration, "syncMode" | "destinationSyncMode">
  ): AirbyteStreamAndConfiguration => ({
    ...streamNode,
    config: { ...streamNode.config, ...config },
  });
  if (!streamNode.stream?.supportedSyncModes) {
    return streamNode;
  }
  const {
    stream: { supportedSyncModes, sourceDefinedCursor, sourceDefinedPrimaryKey },
  } = streamNode;

  if (
    supportedSyncModes.includes(SyncMode.incremental) &&
    supportedDestinationSyncModes.includes(DestinationSyncMode.append_dedup) &&
    sourceDefinedCursor &&
    sourceDefinedPrimaryKey?.length
  ) {
    return updateStreamConfig({
      syncMode: SyncMode.incremental,
      destinationSyncMode: DestinationSyncMode.append_dedup,
    });
  }

  if (supportedDestinationSyncModes.includes(DestinationSyncMode.overwrite)) {
    return updateStreamConfig({
      syncMode: SyncMode.full_refresh,
      destinationSyncMode: DestinationSyncMode.overwrite,
    });
  }

  if (
    supportedSyncModes.includes(SyncMode.incremental) &&
    supportedDestinationSyncModes.includes(DestinationSyncMode.append)
  ) {
    return updateStreamConfig({
      syncMode: SyncMode.incremental,
      destinationSyncMode: DestinationSyncMode.append,
    });
  }

  if (supportedDestinationSyncModes.includes(DestinationSyncMode.append)) {
    return updateStreamConfig({
      syncMode: SyncMode.full_refresh,
      destinationSyncMode: DestinationSyncMode.append,
    });
  }
  return streamNode;
};

/**
 * will be removed soon, but part of functionality is extracted to analyzeSyncCatalogBreakingChanges
 * @deprecated
 */
export const calculateInitialCatalog = (
  schema: AirbyteCatalog,
  supportedDestinationSyncModes: DestinationSyncMode[],
  streamsWithBreakingFieldChanges?: StreamTransform[],
  isNotCreateMode?: boolean,
  newStreamDescriptors?: StreamDescriptor[]
): AirbyteCatalog => {
  return {
    streams: schema.streams.map<AirbyteStreamAndConfiguration>((apiNode) => {
      const nodeStream = verifySourceDefinedProperties(verifySupportedSyncModes(apiNode), isNotCreateMode || false);

      // if the stream is new since a refresh, verify cursor and get optimal sync modes
      const isStreamNew = newStreamDescriptors?.some(
        (streamIdFromDiff) =>
          streamIdFromDiff.name === nodeStream.stream?.name &&
          streamIdFromDiff.namespace === nodeStream.stream?.namespace
      );

      // if we're in edit or readonly mode and the stream is not new, check for breaking changes then return
      if (isNotCreateMode && !isStreamNew) {
        // narrow down the breaking field changes from this connection to only those relevant to this stream
        const breakingChangesByStream =
          streamsWithBreakingFieldChanges && streamsWithBreakingFieldChanges.length > 0
            ? streamsWithBreakingFieldChanges.filter(({ streamDescriptor }) => {
                return (
                  streamDescriptor.name === nodeStream.stream?.name &&
                  streamDescriptor.namespace === nodeStream.stream?.namespace
                );
              })
            : [];

        return clearBreakingFieldChanges(nodeStream, breakingChangesByStream);
      }

      return getOptimalSyncMode(verifyConfigCursorField(nodeStream), supportedDestinationSyncModes);
    }),
  };
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
        streamTransform.updateStream?.filter((fieldTransform) => fieldTransform.breaking)
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
