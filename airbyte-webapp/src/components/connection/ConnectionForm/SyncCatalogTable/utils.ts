import { CoreOptions, Row } from "@tanstack/react-table";
import isEqual from "lodash/isEqual";

import {
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
  SelectedFieldInfo,
} from "core/api/types/AirbyteClient";
import { Path, SyncSchemaField, SyncSchemaFieldObject, traverseSchemaToField } from "core/domain/catalog";

import { SyncCatalogUIModel } from "./SyncCatalogTable";
import { StatusToDisplay } from "../../syncCatalog/StreamsConfigTable/useStreamsConfigTableRowProps";
import { compareObjectsByFields, flattenSyncSchemaFields, getFieldPathDisplayName } from "../../syncCatalog/utils";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../formConfig";
import { isSameSyncStream } from "../utils";

// Streams
export const getStreamRows = (
  streams: SyncStreamFieldWithId[],
  initialStreams: FormConnectionFormValues["syncCatalog"]["streams"],
  prefix?: string
) =>
  streams.map((streamNode) => {
    const traversedFields = traverseSchemaToField(streamNode.stream?.jsonSchema, streamNode.stream?.name);
    const flattenedFields = flattenSyncSchemaFields(traversedFields);

    const initialStreamNode = initialStreams.find((item) =>
      isSameSyncStream(item, streamNode.stream?.name, streamNode.stream?.namespace)
    );

    return {
      streamNode,
      initialStreamNode,
      name: `${prefix ? prefix : ""}${streamNode.stream?.name || ""}`,
      namespace: streamNode.stream?.namespace || "",
      isEnabled: streamNode.config?.selected,
      traversedFields,
      flattenedFields,
    };
  });

export const isStreamRow = (row: Row<SyncCatalogUIModel>) => row.depth === 0;

/**
 * react-table subRows should have the same structure as parent row(stream)
 * that's why some props in SyncCatalogUIModel are optional
 */
export const getStreamFieldRows: CoreOptions<SyncCatalogUIModel>["getSubRows"] = (row) =>
  row?.flattenedFields?.map((field) => ({
    streamNode: row.streamNode,
    initialStreamNode: row.initialStreamNode,
    name: getFieldPathDisplayName(field.path), // need for global filtering
    field,
    traversedFields: row.traversedFields, // we need all traversed fields for updating field
  }));

// Stream  Fields
/*
 * Check is stream field is selected(enabled) for sync
 */
export const checkIsFieldSelected = (field: SyncSchemaField, config: AirbyteStreamConfiguration): boolean => {
  // If the stream is disabled, effectively each field is unselected
  if (!config?.selected) {
    return false;
  }

  // All fields are implicitly selected if field selection is disabled
  if (!config?.fieldSelectionEnabled) {
    return true;
  }

  // path[0] is the top-level field name for all nested fields
  return !!config?.selectedFields?.find((f) => isEqual(f.fieldPath, [field.path[0]]));
};

export const pathDisplayName = (path: Path): string => path.join(".");

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

/**
 * Get change status for field: added, removed, unchanged, disabled
 * @param initialStreamNode
 * @param streamNode
 * @param field
 */
export const getFieldChangeStatus = (
  initialStreamNode: AirbyteStreamAndConfiguration,
  streamNode: SyncStreamFieldWithId,
  field?: SyncSchemaField
): Exclude<StatusToDisplay, "changed"> => {
  // if stream is disabled then disable all fields
  if (!streamNode.config?.selected) {
    return "disabled";
  }

  // don't get status for nested fields
  if (!field || SyncSchemaFieldObject.isNestedField(field)) {
    return "unchanged";
  }

  const findField = (f: SelectedFieldInfo) => isEqual(f.fieldPath, field.path);

  const fieldExistInSelectedFields = streamNode?.config?.selectedFields?.find(findField);
  const fieldExistsInSelectedFieldsInitialValue = initialStreamNode?.config?.selectedFields?.find(findField);

  // if initially field selection was enabled
  if (initialStreamNode?.config?.fieldSelectionEnabled) {
    if (streamNode?.config?.fieldSelectionEnabled) {
      if (fieldExistsInSelectedFieldsInitialValue && fieldExistInSelectedFields) {
        return "unchanged";
      }
      if (fieldExistsInSelectedFieldsInitialValue && !fieldExistInSelectedFields) {
        return "removed";
      }

      if (!fieldExistsInSelectedFieldsInitialValue && fieldExistInSelectedFields) {
        return "added";
      }

      return "unchanged";
    }

    if (!streamNode?.config?.fieldSelectionEnabled) {
      return fieldExistsInSelectedFieldsInitialValue ? "unchanged" : "added";
    }
  }

  // if initially field selection was disabled
  if (!initialStreamNode?.config?.fieldSelectionEnabled) {
    if (streamNode?.config?.fieldSelectionEnabled) {
      return fieldExistInSelectedFields ? "unchanged" : "removed";
    }
    if (!streamNode?.config?.fieldSelectionEnabled) {
      return "unchanged";
    }
  }

  return "unchanged";
};

export const getRowChangeStatus = (row: Row<SyncCatalogUIModel>) => {
  const { streamNode, initialStreamNode, field } = row.original;

  if (!initialStreamNode) {
    return {
      rowChangeStatus: "unchanged",
    };
  }

  const rowChangeStatus = isStreamRow(row)
    ? getStreamChangeStatus(initialStreamNode, streamNode)
    : getFieldChangeStatus(initialStreamNode, streamNode, field);

  return {
    rowChangeStatus,
  };
};
