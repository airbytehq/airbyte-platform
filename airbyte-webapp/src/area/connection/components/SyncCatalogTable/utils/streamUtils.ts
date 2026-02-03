import { Row } from "@tanstack/react-table";

import { SyncStreamFieldWithId, FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { isSameSyncStream } from "area/connection/components/ConnectionForm/utils";
import { AirbyteStreamAndConfiguration, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField, traverseSchemaToField } from "core/domain/catalog";

import { flattenSyncSchemaFields } from "./fieldUtils";
import { compareObjectsByFields, StatusToDisplay } from "./miscUtils";
import { getFieldPathDisplayName } from "./pkAndCursorUtils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

/**
 * Group streams by namespace
 * @param streams Array of stream nodes
 */
export const getNamespaceGroups = (streams: SyncStreamFieldWithId[]): Record<string, SyncStreamFieldWithId[]> => {
  return streams.reduce((acc: Record<string, SyncStreamFieldWithId[]>, stream) => {
    const namespace = stream.stream?.namespace || ""; // Use empty string if namespace is undefined
    if (!acc[namespace]) {
      acc[namespace] = [];
    }
    acc[namespace].push(stream);
    return acc;
  }, {});
};

/**
 * Prepare all levels of rows: namespace => stream => fields => nestedFields
 * @param streams
 * @param initialStreams
 * @param prefix
 */
const getTraversedFields = (streamNode: SyncStreamFieldWithId) =>
  traverseSchemaToField(streamNode.stream?.jsonSchema, streamNode.stream?.name);

const getInitialStreamNode = (
  initialStreams: FormConnectionFormValues["syncCatalog"]["streams"],
  streamNode: SyncStreamFieldWithId
) => initialStreams.find((item) => isSameSyncStream(item, streamNode.stream?.name, streamNode.stream?.namespace));

const getNestedFieldRows = (
  rowField: SyncSchemaField,
  streamNode: SyncStreamFieldWithId,
  initialStreamNode: AirbyteStreamAndConfiguration | undefined,
  traversedFields: SyncSchemaField[]
) =>
  rowField?.fields &&
  flattenSyncSchemaFields(rowField?.fields)?.map((nestedField) => ({
    rowType: "nestedField" as const,
    streamNode,
    initialStreamNode,
    name: getFieldPathDisplayName(nestedField.path),
    field: nestedField,
    traversedFields,
  }));

const getFieldRows = (
  traversedFields: SyncSchemaField[],
  streamNode: SyncStreamFieldWithId,
  initialStreamNode: AirbyteStreamAndConfiguration | undefined
) =>
  traversedFields.map((rowField) => ({
    rowType: "field" as const,
    streamNode,
    initialStreamNode,
    name: getFieldPathDisplayName(rowField.path),
    field: rowField,
    traversedFields,
    subRows: getNestedFieldRows(rowField, streamNode, initialStreamNode, traversedFields),
  }));

const getStreamRows = (
  groupedStreams: SyncStreamFieldWithId[],
  initialStreams: FormConnectionFormValues["syncCatalog"]["streams"],
  prefix?: string
) =>
  groupedStreams.map((streamNode) => {
    const traversedFields = getTraversedFields(streamNode);
    const initialStreamNode = getInitialStreamNode(initialStreams, streamNode);

    return {
      rowType: "stream" as const,
      streamNode,
      initialStreamNode,
      name: `${prefix ? prefix : ""}${streamNode.stream?.name || ""}`,
      namespace: streamNode.stream?.namespace || "",
      isEnabled: streamNode.config?.selected,
      traversedFields, // we need all traversed fields for updating field
      subRows: getFieldRows(traversedFields, streamNode, initialStreamNode),
    };
  });

export const getSyncCatalogRows = (
  streams: SyncStreamFieldWithId[],
  initialStreams: FormConnectionFormValues["syncCatalog"]["streams"],
  prefix?: string
) => {
  const namespaceGroups = getNamespaceGroups(streams);

  return Object.entries(namespaceGroups).map(([namespace, groupedStreams]) => ({
    rowType: "namespace" as const,
    name: namespace,
    subRows: getStreamRows(groupedStreams, initialStreams, prefix),
  }));
};

/**
 * Check if row is stream
 * @param row
 */
export const isStreamRow = (row: Row<SyncCatalogUIModel>) => row.depth === 1 && row.original.rowType === "stream";

/**
 * Get change status  for stream: added, removed, changed, unchanged, disabled
 * @param initialStreamNode
 * @param streamNode
 */
export const getStreamChangeStatus = (
  initialStreamNode: AirbyteStreamAndConfiguration,
  streamNode: SyncStreamFieldWithId
): StatusToDisplay => {
  const isStreamEnabled = streamNode.config?.selected;
  const streamStatusChanged = initialStreamNode?.config?.selected !== streamNode.config?.selected;

  const streamChanged = !compareObjectsByFields<AirbyteStreamConfiguration>(
    initialStreamNode?.config,
    streamNode.config,
    ["syncMode", "destinationSyncMode", "cursorField", "primaryKey", "selectedFields", "fieldSelectionEnabled"]
  );

  if (!isStreamEnabled && !streamStatusChanged) {
    return "disabled";
  } else if (streamStatusChanged) {
    return isStreamEnabled ? "added" : "removed";
  } else if (streamChanged) {
    return "changed";
  }
  return "unchanged";
};
