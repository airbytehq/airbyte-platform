import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { SwitchNext } from "components/ui/SwitchNext";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectionStatus, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./ConnectionHeaderControls.module.scss";
import { FormattedScheduleDataMessage } from "./FormattedScheduleDataMessage";
import { useConnectionStatus } from "../ConnectionStatus/useConnectionStatus";
import { useConnectionSyncContext } from "../ConnectionSync/ConnectionSyncContext";
import { FreeHistoricalSyncIndicator } from "../EnabledControl/FreeHistoricalSyncIndicator";

export const ConnectionHeaderControls: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { connection, updateConnectionStatus, connectionUpdating, schemaRefreshing } = useConnectionEditService();
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const navigate = useNavigate();
  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");
  const isReadOnly = mode === "readonly";

  const {
    syncStarting,
    cancelStarting,
    cancelJob,
    syncConnection,
    connectionEnabled,
    clearStarting: resetStarting,
    refreshStarting,
    jobClearRunning: jobResetRunning,
    jobRefreshRunning,
  } = useConnectionSyncContext();

  const onScheduleBtnClick = () => {
    navigate(`${ConnectionRoutePaths.Settings}`, {
      state: { action: "scheduleType" },
    });
  };

  const onChangeStatus = async (checked: boolean) =>
    await updateConnectionStatus(checked ? ConnectionStatus.active : ConnectionStatus.inactive);

  const isDisabled =
    isReadOnly ||
    syncStarting ||
    cancelStarting ||
    resetStarting ||
    refreshStarting ||
    schemaRefreshing ||
    connectionUpdating;
  const isStartSyncBtnDisabled = isDisabled || !connectionEnabled;
  const isSwitchDisabled = isDisabled || hasBreakingSchemaChange;

  return (
    <FlexContainer alignItems="center" gap="none">
      <FreeHistoricalSyncIndicator />
      <Tooltip
        control={
          <Button
            icon="clockOutline"
            variant="clear"
            data-testid="schedule-button"
            className={styles.scheduleButton}
            onClick={onScheduleBtnClick}
            disabled={isDisabled}
          >
            <FormattedScheduleDataMessage
              scheduleType={connection.scheduleType}
              scheduleData={connection.scheduleData}
            />
          </Button>
        }
        placement="top"
      >
        <FormattedMessage id="connection.header.frequency.tooltip" />
      </Tooltip>
      {connectionStatus.status !== ConnectionSyncStatus.running && (
        <Button
          onClick={syncConnection}
          variant="clear"
          data-testid="manual-sync-button"
          disabled={isStartSyncBtnDisabled}
          icon={syncStarting ? "loading" : "sync"}
          iconSize="sm"
          iconColor="primary"
        >
          <Text size="md" color="blue" bold>
            <FormattedMessage id="connection.startSync" />
          </Text>
        </Button>
      )}
      {connectionStatus.status === ConnectionSyncStatus.running && cancelJob && (
        <Button
          onClick={cancelJob}
          disabled={isDisabled}
          data-testid="cancel-sync-button"
          variant="clear"
          icon={cancelStarting ? "loading" : "cross"}
          iconColor="error"
        >
          <Text size="md" color="red" bold>
            <FormattedMessage
              id={
                resetStarting || jobResetRunning
                  ? "connection.cancelDataClear"
                  : jobRefreshRunning || refreshStarting
                  ? "connection.cancelRefresh"
                  : "connection.cancelSync"
              }
            />
          </Text>
        </Button>
      )}
      <Box p="md">
        <SwitchNext
          onChange={onChangeStatus}
          checked={connection.status === ConnectionStatus.active}
          loading={connectionUpdating}
          disabled={isSwitchDisabled}
          className={styles.switch}
          testId="connection-status-switch"
        />
      </Box>
    </FlexContainer>
  );
};
