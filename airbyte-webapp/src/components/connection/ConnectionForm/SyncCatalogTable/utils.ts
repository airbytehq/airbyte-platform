import { CoreOptions, Row } from "@tanstack/react-table";
import isEqual from "lodash/isEqual";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { Path, SyncSchemaField, traverseSchemaToField } from "core/domain/catalog";

import { SyncCatalogUIModel } from "./SyncCatalogTable";
import { flattenSyncSchemaFields, getFieldPathDisplayName } from "../../syncCatalog/utils";
import { SyncStreamFieldWithId } from "../formConfig";

// Streams
export const getStreamRows = (streams: SyncStreamFieldWithId[], prefix?: string) =>
  streams.map((streamNode) => {
    const traversedFields = traverseSchemaToField(streamNode.stream?.jsonSchema, streamNode.stream?.name);
    const flattenedFields = flattenSyncSchemaFields(traversedFields);

    return {
      streamNode,
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
