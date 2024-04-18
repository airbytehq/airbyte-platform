import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { SwitchNext } from "components/ui/SwitchNext";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./ConnectionHeaderControls.module.scss";
import { FormattedScheduleDataMessage } from "./FormattedScheduleDataMessage";
import { useConnectionStatus } from "../ConnectionStatus/useConnectionStatus";
import { useConnectionSyncContext } from "../ConnectionSync/ConnectionSyncContext";
import { FreeHistoricalSyncIndicator } from "../EnabledControl/FreeHistoricalSyncIndicator";

export const ConnectionHeaderControls: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { connection, updateConnectionStatus, connectionUpdating } = useConnectionEditService();
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const navigate = useNavigate();
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);

  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");
  const isReadOnly = mode === "readonly";

  const { syncStarting, cancelStarting, cancelJob, syncConnection, connectionEnabled, resetStarting, jobResetRunning } =
    useConnectionSyncContext();

  const onScheduleBtnClick = () => {
    navigate(`${ConnectionRoutePaths.Settings}`, {
      state: { action: "scheduleType" },
    });
  };

  const onChangeStatus = async (checked: boolean) =>
    await updateConnectionStatus(checked ? ConnectionStatus.active : ConnectionStatus.inactive);

  const isDisabled = isReadOnly || syncStarting || cancelStarting || resetStarting;
  const isStartSyncBtnDisabled = isDisabled || !connectionEnabled;
  const isCancelBtnDisabled = isDisabled || connectionUpdating;
  const isSwitchDisabled = isDisabled || hasBreakingSchemaChange;

  return (
    <FlexContainer alignItems="center" gap="none">
      <FreeHistoricalSyncIndicator />
      <Tooltip
        control={
          <Button icon="clockOutline" variant="clear" onClick={onScheduleBtnClick}>
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
      {!connectionStatus.isRunning && (
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
      {connectionStatus.isRunning && cancelJob && (
        <Button
          onClick={cancelJob}
          disabled={isCancelBtnDisabled}
          variant="clear"
          icon={cancelStarting ? "loading" : "cross"}
          iconColor="error"
        >
          <Text size="md" color="red" bold>
            <FormattedMessage
              id={
                resetStarting || jobResetRunning
                  ? sayClearInsteadOfReset
                    ? "connection.cancelDataClear"
                    : "connection.cancelReset"
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
        />
      </Box>
    </FlexContainer>
  );
};
