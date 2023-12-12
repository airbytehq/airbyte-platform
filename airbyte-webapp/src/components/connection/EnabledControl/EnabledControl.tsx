import classNames from "classnames";
import React from "react";
import { useIntl } from "react-intl";
import { useAsyncFn } from "react-use";

import { Switch } from "components/ui/Switch";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { getFrequencyFromScheduleData, Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import styles from "./EnabledControl.module.scss";
import { FreeHistoricalSyncIndicator } from "./FreeHistoricalSyncIndicator";

interface EnabledControlProps {
  disabled?: boolean;
}

export const EnabledControl: React.FC<EnabledControlProps> = ({ disabled }) => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();

  const { connection, updateConnection, connectionUpdating } = useConnectionEditService();

  const [{ loading }, onChangeStatus] = useAsyncFn(async () => {
    await updateConnection({
      connectionId: connection.connectionId,
      status: connection.status === ConnectionStatus.active ? ConnectionStatus.inactive : ConnectionStatus.active,
    });

    const trackableAction = connection.status === ConnectionStatus.active ? Action.DISABLE : Action.REENABLE;

    analyticsService.track(Namespace.CONNECTION, trackableAction, {
      actionDescription: `${trackableAction} connection`,
      connector_source: connection.source?.sourceName,
      connector_source_definition_id: connection.source?.sourceDefinitionId,
      connector_destination: connection.destination?.destinationName,
      connector_destination_definition_id: connection.destination?.destinationDefinitionId,
      frequency: getFrequencyFromScheduleData(connection.scheduleData),
    });
  }, [
    analyticsService,
    connection.connectionId,
    connection.destination?.destinationDefinitionId,
    connection.destination?.destinationName,
    connection.source?.sourceDefinitionId,
    connection.source?.sourceName,
    connection.status,
    updateConnection,
  ]);

  const isSwitchDisabled = disabled || connectionUpdating;

  return (
    <div className={styles.container} data-testid="enabledControl">
      <FreeHistoricalSyncIndicator />
      <label
        htmlFor="toggle-enabled-source"
        className={classNames(styles.label, { [styles.disabled]: isSwitchDisabled })}
      >
        {formatMessage({ id: connection.status === ConnectionStatus.active ? "tables.enabled" : "tables.disabled" })}
      </label>
      <Switch
        disabled={isSwitchDisabled}
        onChange={onChangeStatus}
        checked={connection.status === ConnectionStatus.active}
        loading={loading}
        id="toggle-enabled-source"
        data-testid="enabledControl-switch"
      />
    </div>
  );
};
