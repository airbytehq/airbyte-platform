import { FormikErrors, getIn } from "formik";
import React, { memo, useCallback, useMemo } from "react";
import { useToggle } from "react-use";

import { ConnectionFormValues, SUPPORTED_MODES } from "components/connection/ConnectionForm/formConfig";

import { SyncSchemaField, SyncSchemaFieldObject, SyncSchemaStream } from "core/domain/catalog";
import { traverseSchemaToField } from "core/domain/catalog/traverseSchemaToField";
import {
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  NamespaceDefinitionType,
  SyncMode,
} from "core/request/AirbyteClient";
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
import { StreamDetailsPanel } from "../StreamDetailsPanel/StreamDetailsPanel";
import { StreamsConfigTableRow } from "../StreamsConfigTable";
import { SyncModeValue } from "../SyncModeSelect";
import { flattenSyncSchemaFields, getFieldPathType } from "../utils";

interface SyncCatalogRowProps {
  streamNode: SyncSchemaStream;
  errors: FormikErrors<ConnectionFormValues>;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat: string;
  prefix: string;
  updateStream: (id: string | undefined, newConfiguration: Partial<AirbyteStreamConfiguration>) => void;
}

const SyncCatalogRowInner: React.FC<SyncCatalogRowProps> = ({
  streamNode,
  updateStream,
  namespaceDefinition,
  namespaceFormat,
  prefix,
  errors,
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
    (config: Partial<AirbyteStreamConfiguration>) => updateStream(streamNode.id, config),
    [updateStream, streamNode]
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

  const destName = prefix + (streamNode.stream?.name ?? "");
  const configErrors = getIn(errors, `syncCatalog.streams[${streamNode.id}].config`);
  const hasError = configErrors && Object.keys(configErrors).length > 0;
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
        hasError={hasError}
        configErrors={configErrors}
        disabled={disabled}
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

export const SyncCatalogRow = memo(SyncCatalogRowInner);
