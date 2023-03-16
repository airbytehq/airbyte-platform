import intersection from "lodash/intersection";

import { SUPPORTED_MODES } from "components/connection/ConnectionForm/formConfig";

import { SyncSchemaField, SyncSchemaFieldObject, SyncSchemaStream, traverseSchemaToField } from "core/domain/catalog";
import { AirbyteStreamConfiguration, DestinationSyncMode } from "core/request/AirbyteClient";

import { pathDisplayName } from "../../PathPopout";
import { flatten } from "../../utils";
import { SyncModeOption, SyncModeValue } from "../SyncModeSelect";

export const getAvailableSyncModesOptions = (
  nodes: SyncSchemaStream[],
  syncModes?: DestinationSyncMode[]
): SyncModeOption[] =>
  SUPPORTED_MODES.filter(([syncMode, destinationSyncMode]) => {
    const supportableModes = intersection(...nodes.map((n) => n.stream?.supportedSyncModes));
    return supportableModes.includes(syncMode) && syncModes?.includes(destinationSyncMode);
  }).map(([syncMode, destinationSyncMode]) => ({
    value: { syncMode, destinationSyncMode },
  }));

export function calculateSharedFields(selectedBatchNodes: SyncSchemaStream[]) {
  const primitiveFieldsByStream = selectedBatchNodes.map(({ stream }) => {
    const traversedFields = traverseSchemaToField(stream?.jsonSchema, stream?.name);
    const flattenedFields = flatten(traversedFields);

    return flattenedFields.filter(SyncSchemaFieldObject.isPrimitive);
  });

  const pathMap = new Map<string, SyncSchemaField>();

  // calculate intersection of primitive fields across all selected streams
  primitiveFieldsByStream.forEach((fields, index) => {
    if (index === 0) {
      fields.forEach((field) => pathMap.set(pathDisplayName(field.path), field));
    } else {
      const fieldMap = new Set(fields.map((f) => pathDisplayName(f.path)));
      pathMap.forEach((_, k) => (!fieldMap.has(k) ? pathMap.delete(k) : null));
    }
  });

  return Array.from(pathMap.values());
}

export const calculateSyncSwitchState = (
  selectedBatchNodes: SyncSchemaStream[],
  options: Partial<AirbyteStreamConfiguration>
) => {
  const numStreamsSelected = selectedBatchNodes.length;

  const numStreamsSelectedWithToggledSync = selectedBatchNodes.filter((n) => n.config?.selected).length;
  const isSelectedExistInOptions = "selected" in options;
  const syncSwitchChecked = isSelectedExistInOptions
    ? options.selected
    : numStreamsSelected === numStreamsSelectedWithToggledSync;
  const syncSwitchMixed =
    !isSelectedExistInOptions &&
    numStreamsSelected !== numStreamsSelectedWithToggledSync &&
    numStreamsSelectedWithToggledSync > 0;

  return { syncSwitchChecked, syncSwitchMixed };
};

export const calculateSelectedSyncMode = (
  selectedBatchNodes: SyncSchemaStream[],
  options: Partial<AirbyteStreamConfiguration>
): Partial<SyncModeValue> => {
  const { syncMode, destinationSyncMode } = options;

  if (syncMode && destinationSyncMode) {
    return { syncMode, destinationSyncMode };
  }

  if (selectedBatchNodes.length === 0) {
    return {};
  }

  const [first] = selectedBatchNodes;
  const syncModeValue = { syncMode: first.config?.syncMode, destinationSyncMode: first.config?.destinationSyncMode };

  if (selectedBatchNodes.length === 1) {
    return syncModeValue;
  }

  for (let i = 1; i < selectedBatchNodes.length; i++) {
    const node = selectedBatchNodes[i];
    if (
      node.config?.syncMode !== first.config?.syncMode ||
      node.config?.destinationSyncMode !== first.config?.destinationSyncMode
    ) {
      return {};
    }
  }

  return syncModeValue;
};
