import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentWorkspace } from "core/api";
import { useRemoveCloudWorkspace } from "core/api/cloud";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { RoutePaths } from "pages/routePaths";

export const DeleteWorkspaceSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();
  const workspace = useCurrentWorkspace();

  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: removeCloudWorkspace, isLoading: isRemovingCloudWorkspace } = useRemoveCloudWorkspace();

  const deleteCurrentWorkspace = () => {
    openConfirmationModal({
      title: "settings.workspaceSettings.delete.confirmation.title",
      text: "settings.workspaceSettings.delete.confirmation.text",
      submitButtonText: "settings.workspaceSettings.delete.confirmation.submitButtonText",
      onSubmit() {
        closeConfirmationModal();
        removeCloudWorkspace(workspace.workspaceId)
          .then(() => {
            registerNotification({
              id: "settings.workspace.delete.success",
              text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
              type: "success",
            });
            navigate(`/${RoutePaths.Workspaces}`);
          })
          .catch(() => {
            registerNotification({
              id: "settings.workspace.delete.error",
              text: formatMessage({ id: "settings.workspaceSettings.delete.error" }),
              type: "error",
            });
          });
      },
    });
  };

  return (
    <FlexContainer justifyContent="center" alignItems="center">
      <Box p="2xl">
        <Button isLoading={isRemovingCloudWorkspace} variant="danger" onClick={deleteCurrentWorkspace}>
          <FormattedMessage id="settings.generalSettings.deleteLabel" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
