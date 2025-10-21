import { useCallback, useMemo } from "react";

import { useLocalStorage } from "./useLocalStorage";
import { useOrganizationSubscriptionStatus } from "./useOrganizationSubscriptionStatus";
import { ProFeaturesWarnModal } from "../../components/ProFeaturesWarnModal";
import { useModalService } from "../../hooks/services/Modal";

/**
 * Custom hook to manage ProFeaturesWarnModal display logic for unified trial users and standard plan users.
 * Shows modal once per feature per plan/status combination. If plan or trial status changes,
 * the modal will be shown again with the appropriate variant.
 */
export const useProFeaturesModal = (featureId: string) => {
  const { isUnifiedTrialPlan, isStandardPlan, trialStatus } = useOrganizationSubscriptionStatus();
  const { openModal } = useModalService();
  const [shownFeatures, setShownFeatures] = useLocalStorage("airbyte_pro-features-shown", {});

  /**
   * Generate a unique key representing the current plan and trial status.
   * This key is stored with each feature to track which state it was shown for.
   * Returns null if the user is not on a plan that should show the modal.
   */
  const currentStateKey = useMemo<string | null>(() => {
    if (isUnifiedTrialPlan) {
      return `unified_trial:${trialStatus || "unknown"}`;
    }
    if (isStandardPlan) {
      return "standard";
    }
    return null;
  }, [isUnifiedTrialPlan, isStandardPlan, trialStatus]);

  /**
   * Determine which modal variant to show based on current plan/status.
   */
  const modalVariant = useMemo<"warning" | "upgrade">(() => {
    if (isStandardPlan) {
      return "upgrade";
    }
    if (isUnifiedTrialPlan && trialStatus === "post_trial") {
      return "upgrade";
    }
    return "warning";
  }, [isUnifiedTrialPlan, isStandardPlan, trialStatus]);

  /**
   * Shows the ProFeaturesWarnModal if needed for the specified feature.
   * @returns Promise<boolean> - true if modal was shown, false if not needed
   */
  const showProFeatureModalIfNeeded = useCallback(async (): Promise<boolean> => {
    // Only show modal if user is on a plan that should show it
    if (!currentStateKey) {
      return false;
    }

    // Check if already shown for this exact state (plan + trial status combination)
    const shownForState = shownFeatures[featureId];
    if (shownForState === currentStateKey) {
      return false; // Already shown for this exact state
    }

    // Show the modal with the appropriate variant
    await openModal({
      title: null,
      content: ({ onComplete }) => (
        <ProFeaturesWarnModal onContinue={() => onComplete("success")} variant={modalVariant} />
      ),
      preventCancel: true,
      size: "xl",
    });

    // Store the state key (not just true) so we know which state it was shown for
    setShownFeatures((prev) => ({
      ...prev,
      [featureId]: currentStateKey,
    }));

    return true;
  }, [featureId, currentStateKey, shownFeatures, modalVariant, openModal, setShownFeatures]);

  return { showProFeatureModalIfNeeded };
};
