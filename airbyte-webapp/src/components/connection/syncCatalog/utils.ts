import isEqual from "lodash/isEqual";
import sortBy from "lodash/sortBy";

import { Path, SyncSchemaField } from "core/domain/catalog";

export type FieldPathType = null | "required" | "sourceDefined";

export const flattenSyncSchemaFields = (fArr: SyncSchemaField[], arr: SyncSchemaField[] = []): SyncSchemaField[] =>
  fArr.reduce<SyncSchemaField[]>((acc, f) => {
    acc.push(f);

    if (f.fields?.length) {
      return flattenSyncSchemaFields(f.fields, acc);
    }
    return acc;
  }, arr);

export const getFieldPathType = (required: boolean, shouldDefine: boolean): FieldPathType =>
  required ? (shouldDefine ? "required" : "sourceDefined") : null;

export const getFieldPathDisplayName = (path: Path): string => path.join(".");

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
