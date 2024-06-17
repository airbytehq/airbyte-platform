import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button, ButtonVariant } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./ConnectionSyncButtons.module.scss";
import { useConnectionSyncContext } from "./ConnectionSyncContext";
import { useConnectionStatus } from "../ConnectionStatus/useConnectionStatus";

interface ConnectionSyncButtonsProps {
  buttonText: React.ReactNode;
  variant?: ButtonVariant;
  buttonClassName?: string;
}

enum ContextMenuOptions {
  ClearData = "ClearData",
}

export const ConnectionSyncButtons: React.FC<ConnectionSyncButtonsProps> = ({
  buttonText,
  variant,
  buttonClassName,
}) => {
  const { formatMessage } = useIntl();
  const {
    syncStarting,
    cancelStarting,
    cancelJob,
    syncConnection,
    connectionEnabled,
    clearStreams: resetStreams,
    clearStarting: resetStarting,
    jobClearRunning: jobResetRunning,
    refreshStarting,
    jobRefreshRunning,
  } = useConnectionSyncContext();
  const { mode, connection } = useConnectionFormService();

  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const disableSyncActions =
    syncStarting || resetStarting || refreshStarting || !connectionEnabled || mode === "readonly";

  const clearDataWithModal = useCallback(() => {
    openConfirmationModal({
      title: <FormattedMessage id="connection.actions.clearData.confirm.title" />,
      text: "connection.actions.clearData.confirm.text",
      additionalContent: (
        <Box pt="xl">
          <Text color="grey400">
            <FormattedMessage id="connection.stream.actions.clearData.confirm.additionalText" />
          </Text>
        </Box>
      ),
      submitButtonText: "connection.stream.actions.clearData.confirm.submit",
      cancelButtonText: "connection.stream.actions.clearData.confirm.cancel",
      submitButtonDataId: "clear-data",
      onSubmit: async () => {
        await resetStreams();
        closeConfirmationModal();
      },
    });
  }, [closeConfirmationModal, openConfirmationModal, resetStreams]);

  const handleDropdownMenuOptionClick = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case ContextMenuOptions.ClearData:
        clearDataWithModal();
        break;
    }
  };

  return (
    <FlexContainer gap="sm">
      {!connectionStatus.isRunning && (
        <Button
          onClick={syncConnection}
          {...(syncStarting && { icon: "sync" })}
          variant={variant}
          className={buttonClassName}
          isLoading={syncStarting}
          data-testid="manual-sync-button"
          disabled={disableSyncActions}
        >
          {buttonText}
        </Button>
      )}
      {connectionStatus.isRunning && cancelJob && (
        <Button
          onClick={cancelJob}
          disabled={syncStarting || resetStarting || refreshStarting || mode === "readonly"} // purposefully allowed for disabled connections as an edge case
          isLoading={cancelStarting}
          variant="danger"
          className={buttonClassName}
        >
          <FormattedMessage
            id={
              resetStarting || jobResetRunning
                ? "connection.cancelDataClear"
                : refreshStarting || jobRefreshRunning
                ? "connection.cancelRefresh"
                : "connection.cancelSync"
            }
          />
        </Button>
      )}
      <DropdownMenu
        placement="bottom-end"
        data-testid="job-history-dropdown-menu"
        options={[
          {
            displayName: formatMessage({
              id: "connection.stream.actions.clearData",
            }),
            value: ContextMenuOptions.ClearData,
            disabled: disableSyncActions || connectionStatus.isRunning,
            "data-testid": "reset-data-dropdown-option",
            className: styles.clearDataLabel,
          },
        ]}
        onChange={handleDropdownMenuOptionClick}
      >
        {() => <Button variant="clear" icon="options" />}
      </DropdownMenu>
    </FlexContainer>
  );
};
