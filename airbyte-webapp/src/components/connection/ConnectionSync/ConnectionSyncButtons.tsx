import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button, ButtonVariant } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./ConnectionSyncButtons.module.scss";
import { useConnectionSyncContext } from "./ConnectionSyncContext";

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
    jobSyncRunning,
    jobResetRunning,
  } = useConnectionSyncContext();

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
    <div className={styles.buttons}>
      {!jobSyncRunning && !jobResetRunning && (
        <Button
          onClick={syncConnection}
          icon={syncStarting ? undefined : <Icon type="sync" />}
          variant={variant}
          className={buttonClassName}
          isLoading={syncStarting}
          data-testid="manual-sync-button"
          disabled={syncStarting || resetStarting || !connectionEnabled}
        >
          {buttonText}
        </Button>
      )}
      {(jobSyncRunning || jobResetRunning) && (
        <Button
          onClick={cancelJob}
          disabled={syncStarting || resetStarting}
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
            disabled: jobSyncRunning || jobResetRunning,
            "data-testid": "reset-data-dropdown-option",
          },
        ]}
        onChange={handleDropdownMenuOptionClick}
      >
        {() => <Button variant="clear" icon={<Icon type="options" />} />}
      </DropdownMenu>
    </div>
  );
};
