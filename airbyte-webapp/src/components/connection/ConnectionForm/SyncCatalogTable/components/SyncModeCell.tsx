import { Row } from "@tanstack/react-table";
import React from "react";

import { useGetDestinationDefinitionSpecification } from "core/api";
import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
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
  const analyticsService = useAnalyticsService();
  const { connection, mode } = useConnectionFormService();
  const { supportedDestinationSyncModes } = useGetDestinationDefinitionSpecification(
    connection.destination.destinationId
  );

  if (!row.original.streamNode) {
    return null;
  }

  const { stream, config } = row.original.streamNode;

  const onSelectSyncMode = (syncMode: SyncModeValue) => {
    if (!stream || !config) {
      return;
    }

    const updatedConfig = updateStreamSyncMode(stream, config, syncMode);
    updateStreamField(row.original.streamNode!, updatedConfig);
    analyticsService.track(Namespace.STREAM_SELECTION, Action.SET_SYNC_MODE, {
      actionDescription: "User selected a sync mode for a stream",
      streamNamespace: stream.namespace,
      streamName: stream.name,
      syncMode: syncMode.syncMode,
      destinationSyncMode: syncMode.destinationSyncMode,
    });
  };

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
      data-testid="sync-mode-select"
    />
  ) : null;
};
