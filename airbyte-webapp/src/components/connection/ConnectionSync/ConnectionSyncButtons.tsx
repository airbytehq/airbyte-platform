import classNames from "classnames";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button, ButtonVariant } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionSyncButtons.module.scss";
import { useConnectionSyncContext } from "./ConnectionSyncContext";
import { useConnectionStatus } from "../ConnectionStatus/useConnectionStatus";

interface ConnectionSyncButtonsProps {
  buttonText: React.ReactNode;
  variant?: ButtonVariant;
  buttonClassName?: string;
}

enum ContextMenuOptions {
  ResetData = "ResetData",
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
    resetStreams,
    resetStarting,
    jobResetRunning,
  } = useConnectionSyncContext();
  const { mode, connection } = useConnectionFormService();
  const isReadOnly = mode === "readonly";
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);

  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const resetWithModal = useCallback(() => {
    sayClearInsteadOfReset
      ? openConfirmationModal({
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
          onSubmit: async () => {
            await resetStreams();
            closeConfirmationModal();
          },
        })
      : openConfirmationModal({
          text: `form.resetDataText`,
          title: `form.resetData`,
          submitButtonText: "form.reset",
          cancelButtonText: "form.noNeed",
          onSubmit: async () => {
            await resetStreams();
            closeConfirmationModal();
          },
          submitButtonDataId: "reset",
        });
  }, [closeConfirmationModal, openConfirmationModal, resetStreams, sayClearInsteadOfReset]);

  const handleDropdownMenuOptionClick = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case ContextMenuOptions.ResetData:
        resetWithModal();
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
          disabled={syncStarting || resetStarting || !connectionEnabled || isReadOnly}
        >
          {buttonText}
        </Button>
      )}
      {connectionStatus.isRunning && cancelJob && (
        <Button
          onClick={cancelJob}
          disabled={syncStarting || resetStarting || isReadOnly}
          isLoading={cancelStarting}
          variant="danger"
          className={buttonClassName}
        >
          <FormattedMessage
            id={
              resetStarting || jobResetRunning
                ? sayClearInsteadOfReset
                  ? "connection.cancelDataClear"
                  : "connection.cancelReset"
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
              id: sayClearInsteadOfReset ? "connection.stream.actions.clearData" : "connection.resetData",
            }),
            value: ContextMenuOptions.ResetData,
            disabled:
              connectionStatus.isRunning || connection.status !== ConnectionStatus.active || mode === "readonly",
            "data-testid": "reset-data-dropdown-option",
            className: classNames({ [styles.clearDataLabel]: sayClearInsteadOfReset }),
          },
        ]}
        onChange={handleDropdownMenuOptionClick}
      >
        {() => <Button variant="clear" icon="options" />}
      </DropdownMenu>
    </FlexContainer>
  );
};
