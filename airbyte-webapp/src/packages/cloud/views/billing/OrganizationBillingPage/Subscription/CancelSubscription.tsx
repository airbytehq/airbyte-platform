import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { useCancelSubscription, useCurrentWorkspace, useUnscheduleCancelSubscription } from "core/api";
import { OrganizationSubscriptionInfoRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

interface CancelSubscriptionProps {
  subscription: OrganizationSubscriptionInfoRead;
  disabled: boolean;
}

export const CancelSubscription: React.FC<CancelSubscriptionProps> = ({ disabled, subscription }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const { organizationId } = useCurrentWorkspace();
  const formatError = useFormatError();
  const { isLoading: cancelLoading, mutateAsync: cancelSubscription } = useCancelSubscription(organizationId);
  const { isLoading: unscheduleLoading, mutateAsync: resubscribe } = useUnscheduleCancelSubscription(organizationId);

  const onResubscribe = async () => {
    try {
      await resubscribe();
      registerNotification({
        id: "subscriptionCancelled",
        type: "success",
        text: <FormattedMessage id="settings.organization.billing.resubscribed" />,
      });
    } catch (e) {
      registerNotification({
        id: "subscriptionCancelError",
        type: "error",
        text: formatError(e),
      });
    }
  };

  const onCancelSubscription = async () => {
    openConfirmationModal({
      title: <FormattedMessage id="settings.organization.billing.cancelSubscriptionModalTitle" />,
      submitButtonText: "settings.organization.billing.cancelSubscriptionConfirm",
      cancelButtonText: "settings.organization.billing.cancelSubscriptionCancel",
      text: (
        <Text>
          <FormattedMessage
            id="settings.organization.billing.cancelSubscriptionModalText"
            values={{ endDate: dayjs(subscription.upcomingInvoice?.dueDate).toDate() }}
          />
        </Text>
      ),
      onSubmit: async () => {
        try {
          const result = await cancelSubscription();
          const endDate = dayjs(result.subscriptionEndsAt).toDate();
          registerNotification({
            id: "subscriptionCancelled",
            type: "success",
            text: <FormattedMessage id="settings.organization.billing.subscriptionCancelled" values={{ endDate }} />,
          });
        } catch (e) {
          registerNotification({
            id: "subscriptionCancelError",
            type: "error",
            text: formatError(e),
          });
        }
        closeConfirmationModal();
      },
    });
  };

  const onClick = () => {
    if (subscription.cancellationDate) {
      onResubscribe();
    } else {
      onCancelSubscription();
    }
  };

  return (
    <Button
      isLoading={cancelLoading || unscheduleLoading}
      onClick={onClick}
      disabled={disabled}
      variant="secondary"
      size="xs"
    >
      {subscription.cancellationDate ? (
        <FormattedMessage id="settings.organization.billing.resubscribe" />
      ) : (
        <FormattedMessage id="settings.organization.billing.cancelSubscription" />
      )}
    </Button>
  );
};
