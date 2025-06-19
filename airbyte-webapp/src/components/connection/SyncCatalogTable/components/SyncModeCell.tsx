import { Row } from "@tanstack/react-table";
import React from "react";

import { useGetDestinationDefinitionSpecification } from "core/api";
import { AirbyteStreamConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useFormMode } from "core/services/ui/FormModeContext";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { SyncModeButton } from "./SyncModeButton";
import { ConnectorIds } from "../../../../area/connector/utils";
import { SUPPORTED_MODES, SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { updateStreamSyncMode } from "../utils";

export interface SyncModeValue {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
}

interface SyncModeCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const SyncModeCell: React.FC<SyncModeCellProps> = ({ row, updateStreamField }) => {
  const analyticsService = useAnalyticsService();
  const { connection } = useConnectionFormService();
  // TODO: remove this experiment when db sources all support all sync modes
  // https://github.com/airbytehq/airbyte-internal-issues/issues/13224
  const allowToSupportAllSyncModes =
    useExperiment("connection.allowToSupportAllSyncModes") ||
    connection.source.sourceDefinitionId === ConnectorIds.Sources.MicrosoftSqlServerMssql;
  const { mode } = useFormMode();
  const { supportedDestinationSyncModes } = useGetDestinationDefinitionSpecification(
    connection.destination.destinationId
  );

  if (!row.original.streamNode) {
    return null;
  }

  const { stream, config } = row.original.streamNode;

  const onSelectSyncMode = (syncMode: SyncModeValue) => {
    if (!row.original.streamNode || !stream || !config) {
      return;
    }

    const updatedConfig = updateStreamSyncMode(stream, config, syncMode);
    updateStreamField(row.original.streamNode, updatedConfig);
    analyticsService.track(Namespace.STREAM_SELECTION, Action.SET_SYNC_MODE, {
      actionDescription: "User selected a sync mode for a stream",
      streamNamespace: stream.namespace,
      streamName: stream.name,
      syncMode: syncMode.syncMode,
      destinationSyncMode: syncMode.destinationSyncMode,
    });
  };

  const getSupportedSyncModes = ([syncMode, destinationSyncMode]: [SyncMode, DestinationSyncMode]) =>
    allowToSupportAllSyncModes
      ? true
      : stream?.supportedSyncModes?.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode);

  const availableSyncModes: SyncModeValue[] = SUPPORTED_MODES.filter(getSupportedSyncModes).map(
    ([syncMode, destinationSyncMode]) => ({
      syncMode,
      destinationSyncMode,
    })
  );

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
