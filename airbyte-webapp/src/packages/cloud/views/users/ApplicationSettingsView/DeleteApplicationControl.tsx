import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { useDeleteApplication } from "core/api";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./ActionButton.module.scss";
export const DeleteApplicationControl: React.FC<{ applicationId: string; applicationName: string }> = ({
  applicationId,
  applicationName,
}) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: deleteApplication } = useDeleteApplication();
  const { formatMessage } = useIntl();

  const onDeleteApplicationButtonClick = useCallback(() => {
    openConfirmationModal({
      text: formatMessage({ id: "settings.applications.deletion.confirm" }, { applicationName }),
      title: formatMessage({ id: "settings.applications.deletion.title" }),
      submitButtonText: formatMessage({ id: "settings.applications.deletion.title" }),
      onSubmit: async () => {
        await deleteApplication(applicationId);
        closeConfirmationModal();
      },
      submitButtonDataId: "remove",
    });
  }, [openConfirmationModal, formatMessage, applicationName, deleteApplication, applicationId, closeConfirmationModal]);

  return (
    <Button
      className={styles.actionButton}
      icon={<Icon type="trash" />}
      variant="secondary"
      onClick={onDeleteApplicationButtonClick}
    >
      <FormattedMessage id="settings.applications.table.delete" />
    </Button>
  );
};
