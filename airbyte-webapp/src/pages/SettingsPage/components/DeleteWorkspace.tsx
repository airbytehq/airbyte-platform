import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useDeleteWorkspace } from "core/api";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { RoutePaths } from "pages/routePaths";

export const DeleteWorkspace: React.FC = () => {
  const { formatMessage } = useIntl();
  const workspace = useCurrentWorkspace();
  const navigate = useNavigate();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: deleteWorkspace, isLoading: isDeletingWorkspace } = useDeleteWorkspace();

  const onDeleteClick = () => {
    openConfirmationModal({
      title: formatMessage({ id: "settings.workspaceSettings.deleteLabel" }),
      text: formatMessage({ id: "settings.workspaceSettings.deleteModal.proceed" }),
      additionalContent: (
        <Box pt="lg">
          <Text>
            <FormattedMessage id="settings.workspaceSettings.deleteModal.text" values={{ workspace: workspace.name }} />
          </Text>
        </Box>
      ),
      submitButtonText: formatMessage({ id: "settings.workspaceSettings.deleteModal.confirm" }),
      onSubmit() {
        closeConfirmationModal();
        deleteWorkspace(workspace.workspaceId)
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
      <Box p="xl">
        <Button isLoading={isDeletingWorkspace} variant="danger" onClick={onDeleteClick}>
          <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
