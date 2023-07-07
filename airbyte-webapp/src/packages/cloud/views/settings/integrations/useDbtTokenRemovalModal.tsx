import { useCallback } from "react";
import { useIntl } from "react-intl";

import { useDbtCloudServiceToken } from "core/api/cloud";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

import { cleanedErrorMessage } from "./DbtCloudSettingsView";

export const useDbtTokenRemovalModal = () => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { deleteToken } = useDbtCloudServiceToken();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useCallback(() => {
    openConfirmationModal({
      text: "settings.integrationSettings.dbtCloudSettings.action.delete.modal",
      title: "settings.integrationSettings.dbtCloudSettings.actions.delete.confirm",
      submitButtonText: "settings.integrationSettings.dbtCloudSettings.actions.delete",
      onSubmit: async () => {
        await deleteToken(void 0, {
          onError: (e) => {
            registerNotification({
              id: "dbtCloud/delete-token-failure",
              text: cleanedErrorMessage(e),
              type: "error",
            });
          },
          onSuccess: () => {
            registerNotification({
              id: "dbtCloud/delete-token-success",
              text: formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.actions.delete.success" }),
              type: "success",
            });
          },
        });
        closeConfirmationModal();
      },
      submitButtonDataId: "delete",
    });
  }, [openConfirmationModal, deleteToken, closeConfirmationModal, registerNotification, formatMessage]);
};
