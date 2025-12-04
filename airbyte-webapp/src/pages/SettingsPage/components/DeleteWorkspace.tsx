import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useCurrentWorkspace,
  useDeleteWorkspace,
  useIsLastWorkspaceInOrganization,
  useGetOrganizationSubscriptionInfo,
  useCancelSubscription,
} from "core/api";
import { OrganizationPaymentConfigReadSubscriptionStatus } from "core/api/types/AirbyteClient";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { RoutePaths } from "pages/routePaths";

import styles from "./DeleteWorkspace.module.scss";

export const DeleteWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organizationId = useCurrentOrganizationId();
  const { mutateAsync: cancelSubscription, isLoading: isCancellingSubscription } = useCancelSubscription(
    organizationId || ""
  );
  const { registerNotification } = useNotificationService();
  const navigate = useNavigate();
  const { formatMessage, formatDate } = useIntl();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const redirectPathAfterDeletion = `/${RoutePaths.Organization}/${workspace.organizationId}`;
  const { subscriptionStatus, isStandardPlan } = useOrganizationSubscriptionStatus();

  // Check if this is the last workspace in the organization
  const { isLastWorkspace, isLoading: isCheckingLastWorkspace } = useIsLastWorkspaceInOrganization(
    organizationId,
    workspace.workspaceId
  );

  // Fetch subscription info only if this is the last workspace
  const { data: subscriptionInfo } = useGetOrganizationSubscriptionInfo(organizationId || "", isLastWorkspace);
  const hasActiveSubscription = subscriptionStatus === OrganizationPaymentConfigReadSubscriptionStatus.subscribed;
  const shouldShowCancelSubscriptionWarning = isLastWorkspace && hasActiveSubscription && isStandardPlan;

  // Handle subscription cancellation after workspace deletion
  const handleCancelSubscription = async () => {
    // Only attempt subscription cancellation if this is the last workspace, the organization has an active subscription, and is on Standard plan
    if (organizationId && shouldShowCancelSubscriptionWarning) {
      try {
        await cancelSubscription();
        registerNotification({
          id: "settings.workspace.delete.success",
          text: formatMessage({
            id: "settings.workspaceSettings.delete.success.withSubscriptionCancellation",
          }),
          type: "success",
        });
      } catch (subscriptionError) {
        registerNotification({
          id: "settings.workspace.delete.subscription.cancellation.failed",
          text: formatMessage({
            id: "settings.workspaceSettings.delete.warning.subscriptionCancellationFailed",
          }),
          type: "warning",
        });
      }
    } else {
      registerNotification({
        id: "settings.workspace.delete.success",
        text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
        type: "success",
      });
    }
  };

  const { mutateAsync: deleteWorkspace, isLoading: isDeletingWorkspace } = useDeleteWorkspace();

  const cancellationDate = subscriptionInfo?.upcomingInvoice?.dueDate
    ? formatDate(new Date(subscriptionInfo.upcomingInvoice.dueDate), { dateStyle: "medium" })
    : "";

  const onRemoveWorkspaceClick = () => {
    const modalText = shouldShowCancelSubscriptionWarning ? (
      <Box className={styles.warningBox}>
        <Box pb="md">
          <FormattedMessage id="settings.workspaceSettings.deleteWorkspace.confirmation.text" />
        </Box>

        <FormattedMessage
          id="settings.workspaceSettings.deleteLastWorkspace.confirmation"
          values={{ planName: subscriptionInfo?.name ?? "", date: cancellationDate }}
        />
      </Box>
    ) : (
      "settings.workspaceSettings.deleteWorkspace.confirmation.text"
    );

    openConfirmationModal({
      text: modalText,
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
        await handleCancelSubscription();
        navigate(redirectPathAfterDeletion);
        closeConfirmationModal();
      },
      submitButtonDataId: "reset",
    });
  };

  return (
    <Button
      isLoading={isCheckingLastWorkspace || isDeletingWorkspace || isCancellingSubscription}
      variant="danger"
      onClick={onRemoveWorkspaceClick}
    >
      <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
    </Button>
  );
};
