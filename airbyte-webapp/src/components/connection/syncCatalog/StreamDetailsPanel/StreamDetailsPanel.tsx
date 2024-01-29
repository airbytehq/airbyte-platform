import { Dialog } from "@headlessui/react";
import cloneDeep from "lodash/cloneDeep";
import React, { useCallback, useMemo, useState } from "react";

import { Overlay } from "components/ui/Overlay";

import { AirbyteStream, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import styles from "./StreamDetailsPanel.module.scss";
import { StreamPanelHeader } from "./StreamPanelHeader";
import { StreamFieldsTable } from "../StreamFieldsTable/StreamFieldsTable";
import {
  toggleAllFieldsSelected,
  toggleFieldInPrimaryKey,
  updateCursorField,
  updateFieldSelected,
} from "../SyncCatalog/streamConfigHelpers";
import { updateStreamSyncMode } from "../SyncCatalog/updateStreamSyncMode";
import { SyncModeValue } from "../SyncModeSelect";
import { checkCursorAndPKRequirements, flattenSyncSchemaFields } from "../utils";

interface StreamDetailsPanelProps {
  stream: AirbyteStream;
  config: AirbyteStreamConfiguration;
  fields: SyncSchemaField[];
  disabled?: boolean;
  availableSyncModes: SyncModeValue[];
  onClose: () => void;
  updateStreamWithConfig: (config: AirbyteStreamConfiguration) => void;
}

export const StreamDetailsPanel: React.FC<StreamDetailsPanelProps> = ({
  stream,
  config,
  fields,
  disabled,
  availableSyncModes,
  onClose,
  updateStreamWithConfig,
}) => {
  const [clonedConfig, setClonedConfig] = useState<AirbyteStreamConfiguration>(cloneDeep(config));

  const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;

  const flattenedFields = useMemo(() => flattenSyncSchemaFields(fields), [fields]);

  const { pkRequired, cursorRequired, shouldDefinePk, shouldDefineCursor } = checkCursorAndPKRequirements(
    clonedConfig,
    stream
  );

  // Header handlers
  const onToggleAllFieldsSelected = useCallback(() => {
    const updatedConfig = toggleAllFieldsSelected(clonedConfig);

    setClonedConfig((prevState) => ({
      ...prevState,
      ...updatedConfig,
    }));
  }, [clonedConfig]);

  const onSelectStream = useCallback(
    () =>
      setClonedConfig((prevState) => ({
        ...prevState,
        selected: !(clonedConfig && clonedConfig.selected),
      })),
    [clonedConfig]
  );

  const onSelectSyncMode = useCallback(
    (syncMode: SyncModeValue) => {
      const updatedConfig = updateStreamSyncMode(stream, clonedConfig, syncMode);
      setClonedConfig(updatedConfig);
    },
    [clonedConfig, stream]
  );

  // Table handlers
  const onToggleFieldSelected = useCallback(
    (fieldPath: string[], isSelected: boolean) => {
      const updatedConfig = updateFieldSelected({
        config: clonedConfig,
        fields,
        fieldPath,
        isSelected,
        numberOfFieldsInStream,
      });
      setClonedConfig((prevState) => ({
        ...prevState,
        ...updatedConfig,
      }));
    },
    [clonedConfig, fields, numberOfFieldsInStream]
  );

  const onCursorSelect = useCallback(
    (cursorField: string[]) => {
      const updatedConfig = updateCursorField(clonedConfig, cursorField, numberOfFieldsInStream);
      setClonedConfig((prevState) => ({
        ...prevState,
        ...updatedConfig,
      }));
    },
    [clonedConfig, numberOfFieldsInStream]
  );

  const onPkSelect = useCallback(
    (pkPath: string[]) => {
      const updatedConfig = toggleFieldInPrimaryKey(clonedConfig, pkPath, numberOfFieldsInStream);
      setClonedConfig((prevState) => ({
        ...prevState,
        ...updatedConfig,
      }));
    },
    [clonedConfig, numberOfFieldsInStream]
  );

  // onCLose
  const onCloseStreamDetailsPanel = useCallback(() => {
    updateStreamWithConfig(clonedConfig);
    onClose();
  }, [clonedConfig, onClose, updateStreamWithConfig]);

  return (
    <Dialog open onClose={onCloseStreamDetailsPanel}>
      <Overlay />
      <Dialog.Panel className={styles.container} data-testid="stream-details">
        <StreamPanelHeader
          stream={stream}
          config={clonedConfig}
          disabled={disabled}
          onClose={onCloseStreamDetailsPanel}
          onSelectedChange={onSelectStream}
          onSelectSyncMode={onSelectSyncMode}
          availableSyncModes={availableSyncModes}
        />
        <div className={styles.tableContainer}>
          <StreamFieldsTable
            config={clonedConfig}
            syncSchemaFields={flattenedFields}
            handleFieldToggle={onToggleFieldSelected}
            onCursorSelect={onCursorSelect}
            onPkSelect={onPkSelect}
            shouldDefinePk={shouldDefinePk}
            shouldDefineCursor={shouldDefineCursor}
            isCursorDefinitionSupported={cursorRequired}
            isPKDefinitionSupported={pkRequired}
            toggleAllFieldsSelected={onToggleAllFieldsSelected}
          />
        </div>
      </Dialog.Panel>
    </Dialog>
  );
};
