import { isOrganizationSubscribed, useCurrentOrganizationId, useOrganizationPlan } from "area/organization/utils";
import { useOrgInfo } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

/**
 * Whether the billing page shows the billing alerts section for the current organization.
 */
export const useShowsBillingAlertsSection = (): boolean => {
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  const { isProPlan, isFlexPlan } = useOrganizationPlan();

  return canManageOrganizationBilling && isOrganizationSubscribed(billing) && !isProPlan && !isFlexPlan;
};
