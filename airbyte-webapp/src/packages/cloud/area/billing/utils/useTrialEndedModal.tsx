import { TrialEndedModal } from "components/TrialEndedModal/TrialEndedModal";

import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

export const useTrialEndedModal = (): void => {
  const { openModal } = useModalService();
  const showTeamsFeaturesWarnModal = useExperiment("entitlements.showTeamsFeaturesWarnModal");

  useOrganizationSubscriptionStatus({
    refetchWithInterval: true,
    onSuccessGetTrialStatusClb: (isTrialEndedAndLockedOrDisabled: boolean) => {
      if (showTeamsFeaturesWarnModal && isTrialEndedAndLockedOrDisabled) {
        openModal({
          content: TrialEndedModal,
        });
      }
    },
  });
};
