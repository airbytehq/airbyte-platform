import { useCurrentOrganizationInfo } from "core/api";

import { ADP_PLAN_IDS } from "./organizationPlans";

const adpPlanIdValues = Object.values(ADP_PLAN_IDS) as string[];

export const useIsAdpOrganization = (): boolean => {
  const organizationInfo = useCurrentOrganizationInfo();
  const planId = organizationInfo?.organizationPlanId;
  return planId !== undefined && adpPlanIdValues.includes(planId);
};
