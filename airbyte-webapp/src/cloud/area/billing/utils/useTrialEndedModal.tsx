import { useEffect } from "react";

import { TrialEndedModal } from "area/organization/components/TrialEndedModal/TrialEndedModal";
import { useModalService } from "core/services/Modal";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

// Interval for both fetching trial status and reopening the modal
const TRIAL_STATUS_REFETCH_INTERVAL = 60 * 60 * 1000; // 1 hour

export const useTrialEndedModal = (): void => {
  const { openModal } = useModalService();

  const { trialStatus, isUnifiedTrialPlan } = useOrganizationSubscriptionStatus({
    refetchWithInterval: TRIAL_STATUS_REFETCH_INTERVAL,
  });

  useEffect(() => {
    // Only set up interval if conditions are met
    if (isUnifiedTrialPlan && trialStatus === "post_trial") {
      // Open modal immediately
      openModal({
        content: TrialEndedModal,
      });

      // Then reopen every TRIAL_STATUS_REFETCH_INTERVAL seconds
      const intervalId = setInterval(() => {
        openModal({
          content: TrialEndedModal,
        });
      }, TRIAL_STATUS_REFETCH_INTERVAL);

      // Cleanup: clear interval when conditions change
      return () => clearInterval(intervalId);
    }

    // If conditions not met, no interval to clear
    return undefined;
    // openModal is stable (memoized with empty deps), safe to omit from deps
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trialStatus, isUnifiedTrialPlan]);
};
