import { ColumnFilter, Row } from "@tanstack/react-table";
import isEqual from "lodash/isEqual";
import sortBy from "lodash/sortBy";

import { SelectedFieldInfo } from "core/api/types/AirbyteClient";

import { getFieldChangeStatus } from "./fieldUtils";
import { getStreamChangeStatus, isStreamRow } from "./streamUtils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

/**
 * Find row by id
 * note: don't use getRow() method from react-table instance, when column filters are applied, it will return row by index not by id
 * @param rows
 * @param id
 */
export const findRow = (rows: Array<Row<SyncCatalogUIModel>>, id: string) => rows.find((row) => row.id === id);

/**
 * Is filter by stream enabled
 * @param columnFilters - column filters array, for "stream.selected" column the format is: { id: "stream.selected", value: boolean }
 * @param id - column id
 * @returns boolean - true or false if filter by stream is enabled, undefined if filter is not set
 */
export const getColumnFilterValue = (columnFilters: ColumnFilter[], id: string) =>
  columnFilters.find((filter) => filter.id === id)?.value;

/**
 * Get row change status
 * @param row
 */
export type StatusToDisplay = "disabled" | "added" | "removed" | "changed" | "unchanged";
export const getRowChangeStatus = (row: Row<SyncCatalogUIModel>): { rowChangeStatus: StatusToDisplay } => {
  const { streamNode, initialStreamNode, field } = row.original;

  if (!initialStreamNode || !streamNode) {
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

/**
 * Generate test id for row
 * @param row
 * @example row-depth-0-namespace-public
 * @example row-depth-0-namespace-no-name
 * @example row-depth-1-stream-activities
 * @example row-depth-2-field-id
 */
export const generateTestId = (row: Row<SyncCatalogUIModel>): string => {
  const {
    original: { rowType, name },
    depth,
  } = row;

  switch (rowType) {
    case "namespace":
      return `row-depth-${depth}-namespace-${name || "no-name"}`;
    case "stream":
      return `row-depth-${depth}-stream-${name}`;
    case "field":
      return `row-depth-${depth}-field-${name}`;
    default:
      return `row-unknown-type`;
  }
};

/**
 * compare two objects by the given prop names
 * @param obj1
 * @param obj2
 * @param fieldsToCompare
 * @returns true if the objects are equal by the given props, false otherwise
 */
export const compareObjectsByFields = <T>(
  obj1: T | undefined,
  obj2: T | undefined,
  fieldsToCompare: Array<keyof T>
): boolean => {
  if (!obj1 || !obj2) {
    return false;
  }

  return fieldsToCompare.every((field) => {
    const field1 = obj1[field];
    const field2 = obj2[field];

    const areArraysEqual = (arr1: T[], arr2: T[]): boolean => {
      const sortedArr1 = sortBy(arr1, (item) => JSON.stringify(item));
      const sortedArr2 = sortBy(arr2, (item) => JSON.stringify(item));
      return isEqual(sortedArr1, sortedArr2);
    };

    if (Array.isArray(field1) && Array.isArray(field2)) {
      if (!areArraysEqual(field1, field2)) {
        return false;
      }
    } else if (!isEqual(field1, field2)) {
      return false;
    }

    return true;
  });
};

/**
 * Merges arrays of SelectedFieldInfo, ensuring there are no duplicates
 */
export function mergeFieldPathArrays(...args: SelectedFieldInfo[][]): SelectedFieldInfo[] {
  const set = new Set<string>();

  args.forEach((array) =>
    array.forEach((selectedFieldInfo) => {
      if (selectedFieldInfo.fieldPath) {
        const key = JSON.stringify(selectedFieldInfo.fieldPath);
        set.add(key);
      }
    })
  );

  return Array.from(set).map((key) => ({ fieldPath: JSON.parse(key) }));
}
