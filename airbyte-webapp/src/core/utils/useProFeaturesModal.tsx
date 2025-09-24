import { useCallback, useEffect } from "react";

import { useLocalStorage } from "./useLocalStorage";
import { useOrganizationSubscriptionStatus } from "./useOrganizationSubscriptionStatus";
import { ProFeaturesWarnModal } from "../../components/ProFeaturesWarnModal";
import { useModalService } from "../../hooks/services/Modal";

/**
 * Custom hook to manage ProFeaturesWarnModal display logic for unified trial users.
 * Shows modal only once per feature for trial users and manages storage cleanup when plan changes.
 */
export const useProFeaturesModal = (featureId: string) => {
  const { isUnifiedTrialPlan } = useOrganizationSubscriptionStatus();
  const { openModal } = useModalService();
  const [shownFeatures, setShownFeatures] = useLocalStorage("airbyte_pro-features-shown", {});

  // Clean up storage when switching away from unified trial plan
  useEffect(() => {
    if (!isUnifiedTrialPlan && Object.keys(shownFeatures).length > 0) {
      setShownFeatures({});
    }
  }, [isUnifiedTrialPlan, shownFeatures, setShownFeatures]);

  /**
   * Shows the ProFeaturesWarnModal if needed for the specified feature.
   * @returns Promise<boolean> - true if modal was shown, false if not needed
   */
  const showProFeatureModalIfNeeded = useCallback(async (): Promise<boolean> => {
    // Only show modal for unified trial users
    if (!isUnifiedTrialPlan) {
      return false;
    }

    // Don't show if already shown for this feature
    if (shownFeatures[featureId]) {
      return false;
    }

    // Show the modal
    await openModal({
      title: null,
      content: ({ onComplete }) => <ProFeaturesWarnModal onContinue={() => onComplete("success")} />,
      preventCancel: true,
      size: "xl",
    });

    // Mark feature as shown
    setShownFeatures((prev) => ({
      ...prev,
      [featureId]: true,
    }));

    return true;
  }, [featureId, isUnifiedTrialPlan, shownFeatures, openModal, setShownFeatures]);

  return { showProFeatureModalIfNeeded };
};
