import { TrialEndedModal } from "components/TrialEndedModal/TrialEndedModal";

import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

export const useTrialEndedModal = (): void => {
  const { openModal } = useModalService();
  const showTrialEndedModal = useExperiment("entitlements.showTrialEndedModal");

  useOrganizationSubscriptionStatus({
    refetchWithInterval: true,
    onSuccessGetTrialStatusClb: (isTrialEndedAndLockedOrDisabled: boolean) => {
      if (showTrialEndedModal && isTrialEndedAndLockedOrDisabled) {
        openModal({
          content: TrialEndedModal,
        });
      }
    },
  });
};
