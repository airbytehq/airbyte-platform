import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Switch } from "components/ui/Switch";

import { useSyncConnection, useUpdateConnection } from "core/api";
import { ConnectionStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { Action, Namespace, getFrequencyFromScheduleData, useAnalyticsService } from "core/services/analytics";

interface StatusCellControlProps {
  hasBreakingChange?: boolean;
  enabled?: boolean;
  isSyncing?: boolean;
  isManual?: boolean;
  id: string;
  connection: WebBackendConnectionListItem;
}

export const StatusCellControl: React.FC<StatusCellControlProps> = ({
  enabled,
  isManual,
  id,
  isSyncing,
  hasBreakingChange,
  connection,
}) => {
  const analyticsService = useAnalyticsService();
  const { mutateAsync: updateConnection, isLoading } = useUpdateConnection();
  const { mutateAsync: syncConnection, isLoading: isSyncStarting } = useSyncConnection();

  const onRunManualSync = (event: React.SyntheticEvent) => {
    event.stopPropagation();

    if (connection) {
      syncConnection(connection);
    }
  };

  if (!isManual) {
    const onSwitchChange = async (event: React.SyntheticEvent) => {
      event.stopPropagation();
      await updateConnection({
        connectionId: id,
        status: enabled ? ConnectionStatus.inactive : ConnectionStatus.active,
      }).then((updatedConnection) => {
        const action = updatedConnection.status === ConnectionStatus.active ? Action.REENABLE : Action.DISABLE;

        analyticsService.track(Namespace.CONNECTION, action, {
          frequency: getFrequencyFromScheduleData(connection.scheduleData),
          connector_source: connection.source?.sourceName,
          connector_source_definition_id: connection.source?.sourceDefinitionId,
          connector_destination: connection.destination?.destinationName,
          connector_destination_definition_id: connection.destination?.destinationDefinitionId,
        });
      });
    };

    return (
      // this is so we can stop event propagation so the row doesn't receive the click and redirect
      // eslint-disable-next-line jsx-a11y/no-static-element-interactions
      <div
        onClick={(event: React.SyntheticEvent) => event.stopPropagation()}
        onKeyPress={(event: React.SyntheticEvent) => event.stopPropagation()}
      >
        <Switch
          checked={enabled}
          onChange={onSwitchChange}
          disabled={hasBreakingChange}
          loading={isLoading}
          data-testid="enable-connection-switch"
        />
      </div>
    );
  }

  return (
    <Button
      onClick={onRunManualSync}
      isLoading={isSyncStarting || isSyncing}
      disabled={!enabled || hasBreakingChange || isSyncStarting || isSyncing}
      data-testid="manual-sync-button"
    >
      <FormattedMessage id="connection.startSync" />
    </Button>
  );
};
