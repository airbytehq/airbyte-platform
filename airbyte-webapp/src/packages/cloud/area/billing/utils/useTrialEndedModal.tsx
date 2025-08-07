import { TrialEndedModal } from "components/TrialEndedModal/TrialEndedModal";

import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useModalService } from "hooks/services/Modal";

export const useTrialEndedModal = (): void => {
  const { openModal } = useModalService();

  useOrganizationSubscriptionStatus({
    refetchWithInterval: true,
    onSuccessGetTrialStatusClb: (isTrialEndedAndLockedOrDisabled: boolean) => {
      if (isTrialEndedAndLockedOrDisabled) {
        openModal({
          content: TrialEndedModal,
        });
      }
    },
  });
};
