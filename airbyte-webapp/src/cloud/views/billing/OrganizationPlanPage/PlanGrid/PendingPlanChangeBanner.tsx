import { FormattedDate, FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import { useUnschedulePlanChange } from "core/api";
import { OrganizationSubscriptionInfoReadPendingPlanChange } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useNotificationService } from "core/services/Notification";

interface PendingPlanChangeBannerProps {
  organizationId: string;
  pendingPlanChange: OrganizationSubscriptionInfoReadPendingPlanChange | undefined;
}

export const PendingPlanChangeBanner: React.FC<PendingPlanChangeBannerProps> = ({
  organizationId,
  pendingPlanChange,
}) => {
  const { registerNotification } = useNotificationService();
  const formatError = useFormatError();
  const { isLoading, mutateAsync: unschedulePlanChange } = useUnschedulePlanChange(organizationId);

  if (!pendingPlanChange) {
    return null;
  }

  const onCancelPlanChange = async () => {
    try {
      await unschedulePlanChange();
      registerNotification({
        id: "planChangeUnscheduled",
        type: "success",
        text: <FormattedMessage id="planGrid.pendingPlanChange.cancelled" />,
      });
    } catch (e) {
      registerNotification({
        id: "planChangeUnscheduleError",
        type: "error",
        text: formatError(e),
      });
    }
  };

  return (
    <Message
      type="success"
      text={
        <FormattedMessage
          id="planGrid.pendingPlanChange.banner"
          values={{
            plan: pendingPlanChange.planName,
            date: <FormattedDate value={pendingPlanChange.effectiveDate} month="long" day="numeric" />,
          }}
        />
      }
      actionBtnText={<FormattedMessage id="planGrid.pendingPlanChange.cancel" />}
      onAction={onCancelPlanChange}
      actionBtnProps={{ isLoading }}
      data-testid="pending-plan-change-banner"
    />
  );
};
