import get from "lodash/get";
import set from "lodash/set";
import React, { useCallback, useMemo } from "react";
import { FieldErrors } from "react-hook-form";
import { useToggle } from "react-use";

import { AirbyteStreamConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { SyncSchemaField, SyncSchemaFieldObject } from "core/domain/catalog";
import { traverseSchemaToField } from "core/domain/catalog/traverseSchemaToField";
import { naturalComparatorBy } from "core/utils/objects";
import { useDestinationNamespace } from "hooks/connection/useDestinationNamespace";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import {
  updatePrimaryKey,
  toggleFieldInPrimaryKey,
  updateCursorField,
  updateFieldSelected,
  toggleAllFieldsSelected,
} from "./streamConfigHelpers";
import { updateStreamSyncMode } from "./updateStreamSyncMode";
import { FormConnectionFormValues, SyncStreamFieldWithId, SUPPORTED_MODES } from "../../ConnectionForm/formConfig";
import { StreamDetailsPanel } from "../StreamDetailsPanel/StreamDetailsPanel";
import { StreamsConfigTableRow } from "../StreamsConfigTable/StreamsConfigTableRow";
import { SyncModeValue } from "../SyncModeSelect";
import { flattenSyncSchemaFields, getFieldPathType } from "../utils";

interface SyncCatalogRowProps
  extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat" | "prefix"> {
  streamNode: SyncStreamFieldWithId;
  updateStreamNode: (newStreamNode: SyncStreamFieldWithId) => void;
  errors: FieldErrors<FormConnectionFormValues>;
}

/**
 * react-hook-form sync catalog row component
 */
export const SyncCatalogRow: React.FC<SyncCatalogRowProps & { className?: string }> = ({
  streamNode,
  updateStreamNode,
  namespaceDefinition,
  namespaceFormat,
  prefix,
  errors,
  className,
}) => {
  const { stream, config } = streamNode;

  const fields = useMemo(() => {
    const traversedFields = traverseSchemaToField(stream?.jsonSchema, stream?.name);
    return traversedFields.sort(naturalComparatorBy((field) => field.cleanedName));
  }, [stream?.jsonSchema, stream?.name]);

  // FIXME: Temp fix to return empty object when the json schema does not have .properties
  // This prevents the table from crashing but still will not render the fields in the stream.
  const streamProperties = streamNode?.stream?.jsonSchema?.properties ?? {};
  const numberOfFieldsInStream = Object.keys(streamProperties).length ?? 0;

  const {
    destDefinitionSpecification: { supportedDestinationSyncModes },
  } = useConnectionFormService();
  const { mode } = useConnectionFormService();

  const [isStreamDetailsPanelOpened, setIsStreamDetailsPanelOpened] = useToggle(false);

  const updateStreamWithConfig = useCallback(
    (configObj: Partial<AirbyteStreamConfiguration>) => {
      const updatedStreamNode = set(streamNode, "config", {
        ...streamNode.config,
        ...configObj,
      });

      // config.selectedFields must be removed if fieldSelection is disabled
      if (!updatedStreamNode.config?.fieldSelectionEnabled) {
        delete updatedStreamNode.config?.selectedFields;
      }

      updateStreamNode(updatedStreamNode);
    },
    [streamNode, updateStreamNode]
  );

  const onSelectSyncMode = useCallback(
    (syncMode: SyncModeValue) => {
      if (!streamNode.config || !streamNode.stream) {
        return;
      }
      const updatedConfig = updateStreamSyncMode(streamNode.stream, streamNode.config, syncMode);
      updateStreamWithConfig(updatedConfig);
    },
    [streamNode, updateStreamWithConfig]
  );

  const onSelectStream = useCallback(
    () =>
      updateStreamWithConfig({
        selected: !(config && config.selected),
      }),
    [config, updateStreamWithConfig]
  );

  const onPkSelect = useCallback(
    (pkPath: string[]) => {
      if (!config) {
        return;
      }
      const updatedConfig = toggleFieldInPrimaryKey(config, pkPath, numberOfFieldsInStream);
      updateStreamWithConfig(updatedConfig);
    },
    [config, updateStreamWithConfig, numberOfFieldsInStream]
  );

  const onCursorSelect = useCallback(
    (cursorField: string[]) => {
      if (!config) {
        return;
      }
      const updatedConfig = updateCursorField(config, cursorField, numberOfFieldsInStream);
      updateStreamWithConfig(updatedConfig);
    },
    [config, numberOfFieldsInStream, updateStreamWithConfig]
  );

  const onPkUpdate = useCallback(
    (newPrimaryKey: string[][]) => {
      if (!config) {
        return;
      }
      const updatedConfig = updatePrimaryKey(config, newPrimaryKey, numberOfFieldsInStream);
      updateStreamWithConfig(updatedConfig);
    },
    [config, updateStreamWithConfig, numberOfFieldsInStream]
  );

  const onToggleAllFieldsSelected = useCallback(() => {
    if (!config) {
      return;
    }
    const updatedConfig = toggleAllFieldsSelected(config);
    updateStreamWithConfig(updatedConfig);
  }, [config, updateStreamWithConfig]);

  const onToggleFieldSelected = useCallback(
    (fieldPath: string[], isSelected: boolean) => {
      if (!config) {
        return;
      }
      const updatedConfig = updateFieldSelected({ config, fields, fieldPath, isSelected, numberOfFieldsInStream });
      updateStreamWithConfig(updatedConfig);
    },
    [config, fields, numberOfFieldsInStream, updateStreamWithConfig]
  );

  const pkRequired = config?.destinationSyncMode === DestinationSyncMode.append_dedup;
  const cursorRequired = config?.syncMode === SyncMode.incremental;
  const shouldDefinePk = stream?.sourceDefinedPrimaryKey?.length === 0 && pkRequired;
  const shouldDefineCursor = !stream?.sourceDefinedCursor && cursorRequired;

  const availableSyncModes: SyncModeValue[] = useMemo(
    () =>
      SUPPORTED_MODES.filter(
        ([syncMode, destinationSyncMode]) =>
          stream?.supportedSyncModes?.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode)
      ).map(([syncMode, destinationSyncMode]) => ({
        syncMode,
        destinationSyncMode,
      })),
    [stream?.supportedSyncModes, supportedDestinationSyncModes]
  );

  const destNamespace =
    useDestinationNamespace(
      {
        namespaceDefinition,
        namespaceFormat,
      },
      stream?.namespace
    ) ?? "";

  const flattenedFields = useMemo(() => flattenSyncSchemaFields(fields), [fields]);

  const primitiveFields = useMemo<SyncSchemaField[]>(
    () => flattenedFields.filter(SyncSchemaFieldObject.isPrimitive),
    [flattenedFields]
  );

  const destName = `${prefix ? prefix : ""}${streamNode.stream?.name ?? ""}`;
  const configErrors = get(errors, `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config`);
  const pkType = getFieldPathType(pkRequired, shouldDefinePk);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);
  const hasFields = fields?.length > 0;
  const disabled = mode === "readonly";

  return (
    <>
      <StreamsConfigTableRow
        stream={streamNode}
        destNamespace={destNamespace}
        destName={destName}
        availableSyncModes={availableSyncModes}
        onSelectStream={onSelectStream}
        onSelectSyncMode={onSelectSyncMode}
        primitiveFields={primitiveFields}
        pkType={pkType}
        onPrimaryKeyChange={onPkUpdate}
        cursorType={cursorType}
        onCursorChange={onCursorSelect}
        fields={fields}
        openStreamDetailsPanel={setIsStreamDetailsPanelOpened}
        configErrors={configErrors}
        disabled={disabled}
        className={className}
      />
      {isStreamDetailsPanelOpened && hasFields && (
        <StreamDetailsPanel
          config={config}
          disabled={mode === "readonly"}
          syncSchemaFields={flattenedFields}
          onClose={setIsStreamDetailsPanelOpened}
          onCursorSelect={onCursorSelect}
          onPkSelect={onPkSelect}
          onSelectedChange={onSelectStream}
          onSelectSyncMode={onSelectSyncMode}
          handleFieldToggle={onToggleFieldSelected}
          shouldDefinePk={shouldDefinePk}
          shouldDefineCursor={shouldDefineCursor}
          isCursorDefinitionSupported={cursorRequired}
          isPKDefinitionSupported={pkRequired}
          stream={stream}
          availableSyncModes={availableSyncModes}
          toggleAllFieldsSelected={onToggleAllFieldsSelected}
        />
      )}
    </>
  );
};
