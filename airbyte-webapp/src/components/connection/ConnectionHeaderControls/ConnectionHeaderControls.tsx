import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { SwitchNext } from "components/ui/SwitchNext";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentConnection, useCurrentWorkspace } from "core/api";
import { ConnectionStatus, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { useFormMode } from "core/services/ui/FormModeContext";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./ConnectionHeaderControls.module.scss";
import { FormattedScheduleDataMessage } from "./FormattedScheduleDataMessage";
import { useConnectionStatus } from "../ConnectionStatus/useConnectionStatus";
import { useConnectionSyncContext } from "../ConnectionSync/ConnectionSyncContext";
import { FreeHistoricalSyncIndicator } from "../EnabledControl/FreeHistoricalSyncIndicator";

export const ConnectionHeaderControls: React.FC = () => {
  const { mode } = useFormMode();
  const connection = useCurrentConnection();
  const { updateConnectionStatus, connectionUpdating, schemaRefreshing } = useConnectionEditService();
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const navigate = useNavigate();
  const { workspaceId } = useCurrentWorkspace();
  const connectionStatus = useConnectionStatus(connection.connectionId);
  const canSyncConnection = useGeneratedIntent(Intent.RunAndCancelConnectionSyncAndRefresh);
  const canClearData = useIntent("ClearData", { workspaceId });

  const {
    jobRefreshRunning,
    jobClearRunning,
    syncStarting,
    cancelStarting,
    cancelJob,
    isSyncConnectionAvailable,
    syncConnection,
    clearStarting,
    refreshStarting,
  } = useConnectionSyncContext();

  const onScheduleBtnClick = () => {
    navigate(`${ConnectionRoutePaths.Settings}`, {
      state: { action: "scheduleType" },
    });
  };

  const onChangeStatus = async (checked: boolean) =>
    await updateConnectionStatus(checked ? ConnectionStatus.active : ConnectionStatus.inactive);

  const isActionDisabled =
    !isSyncConnectionAvailable ||
    schemaRefreshing ||
    connectionUpdating ||
    connection.source.isEntitled === false ||
    connection.destination.isEntitled === false;

  const isSyncActionsDisabled = connection.status !== ConnectionStatus.active || !canSyncConnection || isActionDisabled;

  const isSwitchDisabled = mode === "readonly" || schemaRefreshing || connectionUpdating || hasBreakingSchemaChange;

  const isCancelDisabled =
    !canSyncConnection ||
    (!canClearData && jobClearRunning) ||
    syncStarting ||
    cancelStarting ||
    clearStarting ||
    refreshStarting ||
    schemaRefreshing ||
    connectionUpdating;

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
            disabled={mode === "readonly"}
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
          disabled={isSyncActionsDisabled}
          icon={syncStarting || clearStarting || refreshStarting ? "loading" : "sync"}
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
          disabled={isCancelDisabled}
          data-testid="cancel-sync-button"
          variant="clear"
          icon={cancelStarting ? "loading" : "cross"}
          iconColor="error"
        >
          <Text size="md" color="red" bold>
            <FormattedMessage
              id={
                clearStarting || jobClearRunning
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
