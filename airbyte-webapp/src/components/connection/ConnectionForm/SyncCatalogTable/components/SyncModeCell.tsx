import { Row } from "@tanstack/react-table";
import React, { useCallback } from "react";

import { useGetDestinationDefinitionSpecification } from "core/api";
import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { SyncModeButton } from "./SyncModeButton";
import { updateStreamSyncMode } from "../../../syncCatalog/SyncCatalog/updateStreamSyncMode";
import { SyncModeValue } from "../../../syncCatalog/SyncModeSelect";
import { SUPPORTED_MODES, SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface SyncModeCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const SyncModeCell: React.FC<SyncModeCellProps> = ({ row, updateStreamField }) => {
  const { connection, mode } = useConnectionFormService();
  const { supportedDestinationSyncModes } = useGetDestinationDefinitionSpecification(
    connection.destination.destinationId
  );

  const { stream, config } = row.original.streamNode;

  const onSelectSyncMode = useCallback(
    (syncMode: SyncModeValue) => {
      if (!stream || !config) {
        return;
      }

      const updatedConfig = updateStreamSyncMode(stream, config, syncMode);
      updateStreamField(row.original.streamNode, updatedConfig);
    },
    [config, row.original, stream, updateStreamField]
  );

  const availableSyncModes: SyncModeValue[] = SUPPORTED_MODES.filter(
    ([syncMode, destinationSyncMode]) =>
      stream?.supportedSyncModes?.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode)
  ).map(([syncMode, destinationSyncMode]) => ({
    syncMode,
    destinationSyncMode,
  }));

  const syncSchema = config?.syncMode &&
    config?.destinationSyncMode && {
      syncMode: config?.syncMode,
      destinationSyncMode: config?.destinationSyncMode,
    };

  return config?.selected ? (
    <SyncModeButton
      options={availableSyncModes}
      onChange={onSelectSyncMode}
      value={syncSchema}
      disabled={mode === "readonly"}
    />
  ) : null;
};
