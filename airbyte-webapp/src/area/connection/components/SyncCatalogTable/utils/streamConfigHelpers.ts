import isEqual from "lodash/isEqual";

import {
  AirbyteStream,
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  SelectedFieldInfo,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import { mergeFieldPathArrays } from "./miscUtils";
import { isCdcMetaField } from "./pkAndCursorUtils";

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
    // When selecting any regular (non-CDC) field, also auto-select all CDC fields if they exist
    const cdcFields = fields.filter((field) => isCdcMetaField(field.path));

    if (cdcFields.length > 0 && !isCdcMetaField(fieldPath)) {
      // Add the selected field plus all CDC fields
      const cdcFieldInfos = cdcFields.map((field) => ({ fieldPath: field.path }));
      const allSelectedFields = mergeFieldPathArrays(previouslySelectedFields, [{ fieldPath }, ...cdcFieldInfos]);

      // Check if all fields are now selected
      if (allSelectedFields.length === numberOfFieldsInStream) {
        return {
          selectedFields: [],
          fieldSelectionEnabled: false,
        };
      }

      return {
        selectedFields: allSelectedFields,
        fieldSelectionEnabled: true,
      };
    }

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

export function updateStreamSyncMode(
  stream: AirbyteStream,
  config: AirbyteStreamConfiguration,
  syncModes: { syncMode: SyncMode; destinationSyncMode: DestinationSyncMode }
): AirbyteStreamConfiguration {
  const { syncMode, destinationSyncMode } = syncModes;

  // If field selection was enabled, we need to ensure that any source-defined primary key or cursor is selected automatically
  if (config?.fieldSelectionEnabled) {
    const previouslySelectedFields = config?.selectedFields || [];
    const requiredSelectedFields: SelectedFieldInfo[] = [];

    // If the sync mode is incremental, we need to ensure the cursor is selected
    if (syncMode === "incremental") {
      if (stream.sourceDefinedCursor && stream.defaultCursorField?.length) {
        requiredSelectedFields.push({ fieldPath: stream.defaultCursorField });
      }
      if (config.cursorField?.length) {
        requiredSelectedFields.push({ fieldPath: config.cursorField });
      }
    }

    // If the destination sync mode is performs dedup, we need to ensure that each piece of the composite primary key is selected
    if (
      (destinationSyncMode === "append_dedup" || destinationSyncMode === "overwrite_dedup") &&
      stream.sourceDefinedPrimaryKey
    ) {
      if (stream.sourceDefinedPrimaryKey?.length) {
        requiredSelectedFields.push(
          ...stream.sourceDefinedPrimaryKey.map((path) => ({
            fieldPath: path,
          }))
        );
      }
      if (config.primaryKey) {
        requiredSelectedFields.push(
          ...config.primaryKey.map((path) => ({
            fieldPath: path,
          }))
        );
      }
    }

    // Deduplicate the selected fields array, since the same field could have been added twice (e.g. as cursor and pk)
    const selectedFields = mergeFieldPathArrays(previouslySelectedFields, requiredSelectedFields);

    return {
      ...config,
      selectedFields,
      ...syncModes,
    };
  }

  return {
    ...config,
    ...syncModes,
  };
}
