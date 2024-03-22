import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button, ButtonVariant } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

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

  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const resetWithModal = useCallback(() => {
    openConfirmationModal({
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
  }, [closeConfirmationModal, openConfirmationModal, resetStreams]);

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
            id={resetStarting || jobResetRunning ? "connection.cancelReset" : "connection.cancelSync"}
          />
        </Button>
      )}
      <DropdownMenu
        placement="bottom-end"
        data-testid="job-history-dropdown-menu"
        options={[
          {
            displayName: formatMessage({ id: "connection.resetData" }),
            value: ContextMenuOptions.ResetData,
            disabled:
              connectionStatus.isRunning || connection.status !== ConnectionStatus.active || mode === "readonly",
            "data-testid": "reset-data-dropdown-option",
          },
        ]}
        onChange={handleDropdownMenuOptionClick}
      >
        {() => <Button variant="clear" icon="options" />}
      </DropdownMenu>
    </FlexContainer>
  );
};
