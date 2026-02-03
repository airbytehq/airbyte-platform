import { useCallback, useMemo } from "react";

import { ORG_PLAN_IDS } from "components/ui/BrandingBadge/BrandingBadge";

import { ProFeaturesWarnModal } from "area/organization/components/ProFeaturesWarnModal";
import { useModalService } from "core/services/Modal";

import { useLocalStorage } from "./useLocalStorage";
import { useOrganizationSubscriptionStatus } from "./useOrganizationSubscriptionStatus";

/**
 * Custom hook to manage ProFeaturesWarnModal display logic for unified trial users and standard plan users.
 * - Unified Trial: Shows modal once for ANY pro feature (not per feature)
 * - Standard Plan: Shows modal every time for ANY pro feature
 */
export const useProFeaturesModal = (featureId: string) => {
  const { isUnifiedTrialPlan, isStandardPlan } = useOrganizationSubscriptionStatus();
  const { openModal } = useModalService();
  const [shownFeatures, setShownFeatures] = useLocalStorage("airbyte_pro-features-shown", {});

  /**
   * Generate a unique key representing the current plan type.
   * This key is stored with each feature to track which plan it was shown for.
   * Returns null if the user is not on a plan that should show the modal.
   */
  const currentStateKey = useMemo<string | null>(() => {
    if (isUnifiedTrialPlan) {
      return ORG_PLAN_IDS.UNIFIED_TRIAL;
    }
    if (isStandardPlan) {
      return ORG_PLAN_IDS.STANDARD;
    }
    return null;
  }, [isUnifiedTrialPlan, isStandardPlan]);

  /**
   * Determine which modal variant to show based on current plan.
   * - Standard Plan: "upgrade" variant
   * - Unified Trial Plan: "warning" variant
   */
  const modalVariant = useMemo<"warning" | "upgrade">(() => {
    if (isStandardPlan) {
      return "upgrade";
    }
    if (isUnifiedTrialPlan) {
      return "warning";
    }
    return "upgrade";
  }, [isUnifiedTrialPlan, isStandardPlan]);

  /**
   * Shows the ProFeaturesWarnModal if needed for the specified feature.
   * - Unified Trial: Shows once for ANY pro feature (checks if any feature has the unified trial plan value)
   * - Standard Plan: Shows every time for ANY pro feature (no storage check)
   * @returns Promise<boolean> - true if modal was shown, false if not needed
   */
  const showProFeatureModalIfNeeded = useCallback(async (): Promise<boolean> => {
    // Only show modal if user is on a plan that should show it
    if (!currentStateKey) {
      return false;
    }

    // For Unified Trial: check if modal has been shown for ANY feature
    if (isUnifiedTrialPlan) {
      const hasAnyFeatureBeenShown = Object.values(shownFeatures).some((value) => value === ORG_PLAN_IDS.UNIFIED_TRIAL);
      if (hasAnyFeatureBeenShown) {
        return false; // Already shown once for some feature
      }
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

    // Store the feature ID with plan type (only for Unified Trial, to track which feature triggered it)
    if (isUnifiedTrialPlan) {
      setShownFeatures((prev) => ({
        ...prev,
        [featureId]: currentStateKey,
      }));
    }

    return true;
  }, [featureId, currentStateKey, shownFeatures, modalVariant, openModal, setShownFeatures, isUnifiedTrialPlan]);

  return { showProFeatureModalIfNeeded };
};
