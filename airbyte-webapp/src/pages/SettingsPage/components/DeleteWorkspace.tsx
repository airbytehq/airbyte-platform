import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspace, useDeleteWorkspace, useIsLastWorkspaceInOrganization } from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useNotificationService } from "core/services/Notification";
import { RoutePaths } from "pages/routePaths";

export const DeleteWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organizationId = useCurrentOrganizationId();
  const { registerNotification } = useNotificationService();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const redirectPathAfterDeletion = `/${RoutePaths.Organization}/${workspace.organizationId}`;

  // An organization must keep at least one workspace; the server rejects deleting the last one.
  const { isLastWorkspace, isLoading: isCheckingLastWorkspace } = useIsLastWorkspaceInOrganization(
    organizationId,
    workspace.workspaceId
  );

  const { mutateAsync: deleteWorkspace, isLoading: isDeletingWorkspace } = useDeleteWorkspace();

  const onRemoveWorkspaceClick = () => {
    openConfirmationModal({
      text: "settings.workspaceSettings.deleteWorkspace.confirmation.text",
      title: (
        <FormattedMessage
          id="settings.workspaceSettings.deleteWorkspace.confirmation.title"
          values={{ name: workspace.name }}
        />
      ),
      submitButtonText: "settings.workspaceSettings.delete.confirmation.submitButtonText",
      confirmationText: workspace.name,
      onSubmit: async () => {
        await deleteWorkspace(workspace.workspaceId);
        registerNotification({
          id: "settings.workspace.delete.success",
          text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
          type: "success",
        });
        navigate(redirectPathAfterDeletion);
        closeConfirmationModal();
      },
      submitButtonDataId: "reset",
    });
  };

  return (
    <FlexContainer direction="column" gap="sm">
      <Button
        isLoading={isCheckingLastWorkspace || isDeletingWorkspace}
        disabled={isLastWorkspace}
        variant="danger"
        onClick={onRemoveWorkspaceClick}
      >
        <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
      </Button>
      {isLastWorkspace && (
        <Text size="sm" color="grey">
          <FormattedMessage id="settings.workspaceSettings.deleteLastWorkspace.disabled" />
        </Text>
      )}
    </FlexContainer>
  );
};
