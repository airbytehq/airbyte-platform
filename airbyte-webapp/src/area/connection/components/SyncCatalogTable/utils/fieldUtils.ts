import isEqual from "lodash/isEqual";

import { SyncStreamFieldWithId } from "area/connection/components/ConnectionForm/formConfig";
import { isHashingMapper } from "area/connection/utils/mappers";
import {
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
  SelectedFieldInfo,
} from "core/api/types/AirbyteClient";
import { SyncSchemaField, SyncSchemaFieldObject } from "core/domain/catalog";

import { StatusToDisplay } from "./miscUtils";

/**
 * Recursively flattens a nested array of SyncSchemaField objects.
 * @param fArr - The array of SyncSchemaField objects to flatten.
 * @param arr - An optional accumulator array to collect the flattened fields. Defaults to an empty array.
 * @returns A flattened array of SyncSchemaField objects.
 */
export const flattenSyncSchemaFields = (fArr: SyncSchemaField[], arr: SyncSchemaField[] = []): SyncSchemaField[] =>
  fArr.reduce<SyncSchemaField[]>((acc, f) => {
    acc.push(f);

    if (f.fields?.length) {
      return flattenSyncSchemaFields(f.fields, acc);
    }
    return acc;
  }, arr);

/**
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
): StatusToDisplay => {
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

  if (initialStreamNode?.config?.fieldSelectionEnabled) {
    if (streamNode?.config?.fieldSelectionEnabled) {
      if (fieldExistsInSelectedFieldsInitialValue && !fieldExistInSelectedFields) {
        return "removed";
      }

      if (!fieldExistsInSelectedFieldsInitialValue && fieldExistInSelectedFields) {
        return "added";
      }
    }

    // stream field selection was disabled to start with
    // now it is enabled, so if this field was not part
    // of the initial selection it has been added
    if (!streamNode?.config?.fieldSelectionEnabled && !fieldExistsInSelectedFieldsInitialValue) {
      return "added";
    }
  }

  // if initially field selection was disabled
  if (!initialStreamNode?.config?.fieldSelectionEnabled) {
    if (streamNode?.config?.fieldSelectionEnabled && !fieldExistInSelectedFields) {
      return "removed";
    }
  }

  // if the field's hashing was changed
  if (initialStreamNode?.config && streamNode.config) {
    const wasHashed = checkIsFieldHashed(field, initialStreamNode?.config);
    const isHashed = checkIsFieldHashed(field, streamNode?.config);
    if (wasHashed !== isHashed) {
      return "changed";
    }
  }

  return "unchanged";
};

/**
 * Check is stream field is hashed for sync
 */
export const checkIsFieldHashed = (field: SyncSchemaField, config: AirbyteStreamConfiguration): boolean => {
  return (
    config.mappers?.some(
      (mapper) => isHashingMapper(mapper) && isEqual(mapper.mapperConfiguration.targetField, field.path.join("."))
    ) ?? false
  );
};
