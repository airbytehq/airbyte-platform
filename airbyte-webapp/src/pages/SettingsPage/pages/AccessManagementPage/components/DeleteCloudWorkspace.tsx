import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";

import { useCurrentWorkspace } from "core/api";
import { useRemoveCloudWorkspace } from "core/api/cloud";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { RoutePaths } from "pages/routePaths";

export const DeleteCloudWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();

  const { mutateAsync: removeCloudWorkspace, isLoading: isRemovingCloudWorkspace } = useRemoveCloudWorkspace();
  const { registerNotification } = useNotificationService();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const onRemoveWorkspaceClick = () =>
    openConfirmationModal({
      text: `settings.workspaceSettings.deleteWorkspace.confirmation.text`,
      title: (
        <FormattedMessage
          id="settings.workspaceSettings.deleteWorkspace.confirmation.title"
          values={{ name: workspace.name }}
        />
      ),
      submitButtonText: "settings.workspaceSettings.delete.confirmation.submitButtonText",
      confirmationText: workspace.name,
      onSubmit: async () => {
        await removeCloudWorkspace(workspace.workspaceId);
        registerNotification({
          id: "settings.workspace.delete.success",
          text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
          type: "success",
        });
        navigate(`/${RoutePaths.Workspaces}`);
        closeConfirmationModal();
      },
      submitButtonDataId: "reset",
    });

  return (
    <Button isLoading={isRemovingCloudWorkspace} variant="danger" onClick={onRemoveWorkspaceClick}>
      <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
    </Button>
  );
};
