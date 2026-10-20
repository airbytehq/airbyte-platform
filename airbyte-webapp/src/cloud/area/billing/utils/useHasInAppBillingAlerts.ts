import { isOrganizationSubscribed, useCurrentOrganizationId, useOrganizationPlan } from "area/organization/utils";
import { useOrgInfo } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

/**
 * Whether the organization manages its billing alerts in the billing page instead of the Retool form.
 */
export const useHasInAppBillingAlerts = (): boolean => {
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  const isBillingNotificationSettingsEnabled = useExperiment("platform.billing-notification-settings");
  const { isProPlan, isFlexPlan } = useOrganizationPlan();

  return (
    isBillingNotificationSettingsEnabled &&
    canManageOrganizationBilling &&
    isOrganizationSubscribed(billing) &&
    !isProPlan &&
    !isFlexPlan
  );
};
