import { useCurrentOrganizationInfo } from "core/api";

import { ORG_PLAN_IDS } from "./organizationPlans";

export interface UseOrganizationPlanReturn {
  isStiggPlanEnabled: boolean;
  isUnifiedTrialPlan: boolean;
  isStandardTrialPlan: boolean;
  isStandardPlan: boolean;
  isSmePlan: boolean;
  isFlexPlan: boolean;
  isProPlan: boolean;
}

/**
 * Lightweight hook that returns the current organization plan flags.
 * Only calls useCurrentOrganizationInfo — use this instead of useOrganizationSubscriptionStatus
 * when you only need to know the current plan.
 */
export const useOrganizationPlan = (): UseOrganizationPlanReturn => {
  const organizationInfo = useCurrentOrganizationInfo();
  const planId = organizationInfo?.organizationPlanId;

  return {
    isStiggPlanEnabled: !!planId,
    isUnifiedTrialPlan: planId === ORG_PLAN_IDS.UNIFIED_TRIAL,
    isStandardTrialPlan: planId === ORG_PLAN_IDS.STANDARD_TRIAL,
    isStandardPlan: planId === ORG_PLAN_IDS.STANDARD,
    isSmePlan: planId === ORG_PLAN_IDS.SME,
    isFlexPlan: planId === ORG_PLAN_IDS.FLEX,
    isProPlan: planId === ORG_PLAN_IDS.PRO,
  };
};
