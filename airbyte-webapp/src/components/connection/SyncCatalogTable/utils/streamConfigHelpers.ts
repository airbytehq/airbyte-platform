import isEqual from "lodash/isEqual";

import { AirbyteStreamConfiguration, SelectedFieldInfo } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

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

interface onToggleFieldSelectedArguments {
  config: AirbyteStreamConfiguration;
  fields: SyncSchemaField[]; // could be calculated again from config, but is potentially expensive
  fieldPath: string[];
  isSelected: boolean;
  numberOfFieldsInStream: number;
}
export function updateFieldSelected({
  config,
  fields,
  fieldPath,
  isSelected,
  numberOfFieldsInStream,
}: onToggleFieldSelectedArguments): Partial<AirbyteStreamConfiguration> {
  const previouslySelectedFields = config?.selectedFields || [];

  if (!config?.fieldSelectionEnabled && !isSelected) {
    // All fields in a stream are implicitly selected. When deselecting the first one, we also need to explicitly select the rest.
    const allOtherFields = fields.filter((field: SyncSchemaField) => !isEqual(field.path, fieldPath)) ?? [];
    const selectedFields: SelectedFieldInfo[] = allOtherFields.map((field) => ({ fieldPath: field.path }));
    return {
      selectedFields,
      fieldSelectionEnabled: true,
    };
  } else if (isSelected && previouslySelectedFields.length === numberOfFieldsInStream - 1) {
    // In this case we are selecting the only unselected field
    return {
      selectedFields: [],
      fieldSelectionEnabled: false,
    };
  } else if (isSelected) {
    return {
      selectedFields: [...previouslySelectedFields, { fieldPath }],
      fieldSelectionEnabled: true,
    };
  }
  return {
    selectedFields: previouslySelectedFields.filter((f) => !isEqual(f.fieldPath, fieldPath)) || [],
    fieldSelectionEnabled: true,
  };
}

/**
 * If one of the fields in the disabled stream is selected,
 * we need to ensure that mandatory fields are also selected(PK, cursor)
 * @param config
 */
export const getSelectedMandatoryFields = (config: AirbyteStreamConfiguration): SelectedFieldInfo[] => {
  const mandatoryFields: string[][] = [];

  if (!config?.selected) {
    if (config?.primaryKey?.length && ["append_dedup", "overwrite_dedup"].includes(config.destinationSyncMode)) {
      mandatoryFields.push(...config.primaryKey);
    }

    if (config?.cursorField?.length && config.syncMode === "incremental") {
      mandatoryFields.push(config.cursorField);
    }
  }

  return mandatoryFields.map((fieldPath) => ({ fieldPath }));
};

interface updateFieldHashingArguments {
  config: AirbyteStreamConfiguration;
  fieldPath: string[];
  isFieldHashed: boolean;
}
// TODO: cover with tests
export function updateFieldHashing({
  config,
  fieldPath,
  isFieldHashed,
}: updateFieldHashingArguments): Partial<AirbyteStreamConfiguration> {
  if (isFieldHashed) {
    return { hashedFields: [...(config.hashedFields ?? []), { fieldPath }] };
  }

  const nextConfig: Partial<AirbyteStreamConfiguration> = {
    hashedFields: (config.hashedFields ?? []).filter((f) => !isEqual(f.fieldPath, fieldPath)),
  };
  if (nextConfig.hashedFields?.length === 0) {
    nextConfig.hashedFields = undefined;
  }
  return nextConfig;
}

/**
 * Updates the cursor field in AirbyteStreamConfiguration
 */
export function updateCursorField(
  config: AirbyteStreamConfiguration,
  selectedCursorField: string[],
  numberOfFieldsInStream: number
): Partial<AirbyteStreamConfiguration> {
  // If field selection is enabled, we need to be sure the new cursor is also selected
  if (config?.fieldSelectionEnabled) {
    const previouslySelectedFields = config?.selectedFields || [];
    const selectedFields = mergeFieldPathArrays(previouslySelectedFields, [{ fieldPath: selectedCursorField }]);

    // If the number of selected fields is equal to the fields in the stream, field selection is disabled because all fields are selected
    if (selectedFields.length === numberOfFieldsInStream) {
      return { cursorField: selectedCursorField, selectedFields: [], fieldSelectionEnabled: false };
    }

    return {
      fieldSelectionEnabled: true,
      selectedFields,
      cursorField: selectedCursorField,
    };
  }
  return { cursorField: selectedCursorField };
}

/**
 * @deprecated it seems like this function is not used anywhere except tests
 */
export function toggleAllFieldsSelected(config: AirbyteStreamConfiguration): Partial<AirbyteStreamConfiguration> {
  const wasFieldSelectionEnabled = config?.fieldSelectionEnabled;
  const fieldSelectionEnabled = !wasFieldSelectionEnabled;
  const selectedFields: string[][] = [];

  // When deselecting all fields, we need to be careful not to deselect any primary keys or the cursor field
  if (!wasFieldSelectionEnabled) {
    if (
      config?.primaryKey &&
      config.primaryKey.length > 0 &&
      (config.destinationSyncMode === "append_dedup" || config.destinationSyncMode === "overwrite_dedup")
    ) {
      selectedFields.push(...config.primaryKey);
    }
    if (config?.cursorField && config.cursorField.length > 0 && config.syncMode === "incremental") {
      selectedFields.push(config.cursorField);
    }
  }

  return {
    fieldSelectionEnabled,
    selectedFields: selectedFields.map((fieldPath) => ({ fieldPath })),
  };
}

/**
 * Overwrites the entire primaryKey value in AirbyteStreamConfiguration, which is a composite of one or more fieldPaths
 */
export function updatePrimaryKey(
  config: AirbyteStreamConfiguration,
  compositePrimaryKey: string[][],
  numberOfFieldsInStream: number
): Partial<AirbyteStreamConfiguration> {
  // If field selection is enabled, we need to be sure each fieldPath in the new composite primary key is also selected
  if (config?.fieldSelectionEnabled) {
    const previouslySelectedFields = config?.selectedFields || [];
    const selectedFields = mergeFieldPathArrays(
      previouslySelectedFields,
      compositePrimaryKey.map((fieldPath) => ({ fieldPath }))
    );

    // If the number of selected fields is equal to the fields in the stream, field selection is disabled because all fields are selected
    if (selectedFields.length === numberOfFieldsInStream) {
      return { primaryKey: compositePrimaryKey, selectedFields: [], fieldSelectionEnabled: false };
    }

    return {
      fieldSelectionEnabled: true,
      selectedFields,
      primaryKey: compositePrimaryKey,
    };
  }

  return {
    primaryKey: compositePrimaryKey,
  };
}

/**
 * Toggles whether a fieldPath is part of the composite primaryKey
 * @deprecated it seems like this function is not used anywhere except tests
 */
export function toggleFieldInPrimaryKey(
  config: AirbyteStreamConfiguration,
  fieldPath: string[],
  numberOfFieldsInStream: number
): Partial<AirbyteStreamConfiguration> {
  const fieldIsSelected = !config?.primaryKey?.find((pk) => isEqual(pk, fieldPath));
  let newPrimaryKey: string[][];

  if (!fieldIsSelected) {
    newPrimaryKey = config.primaryKey?.filter((key) => !isEqual(key, fieldPath)) ?? [];
  } else {
    newPrimaryKey = [...(config?.primaryKey ?? []), fieldPath];
  }

  // If field selection is enabled, we need to be sure the new fieldPath is also selected
  if (fieldIsSelected && config?.fieldSelectionEnabled) {
    const previouslySelectedFields = config?.selectedFields || [];
    const selectedFields = mergeFieldPathArrays(previouslySelectedFields, [{ fieldPath }]);

    // If the number of selected fields is equal to the fields in the stream, field selection is disabled because all fields are selected
    if (selectedFields.length === numberOfFieldsInStream) {
      return { primaryKey: newPrimaryKey, selectedFields: [], fieldSelectionEnabled: false };
    }

    return {
      fieldSelectionEnabled: true,
      selectedFields,
      primaryKey: newPrimaryKey,
    };
  }

  return {
    primaryKey: newPrimaryKey,
  };
}
