import dayjs from "dayjs";
import { useMemo } from "react";

import { ORG_PLAN_IDS } from "components/ui/BrandingBadge/BrandingBadge";

import { useOrganizationTrialStatus, useOrgInfo, useCurrentOrganizationInfo } from "core/api";
import {
  OrganizationTrialStatusReadTrialStatus,
  OrganizationPaymentConfigReadPaymentStatus,
  OrganizationPaymentConfigReadSubscriptionStatus,
  ISO8601DateTime,
  OrganizationTrialStatusRead,
} from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export interface UseOrganizationSubscriptionStatusReturn {
  // Organization plan information
  isUnifiedTrialPlan: boolean;
  isStandardTrialPlan: boolean;
  isStandardPlan: boolean;

  // Trial Information
  trialStatus: OrganizationTrialStatusReadTrialStatus | undefined;
  trialEndsAt: ISO8601DateTime | undefined;
  isInTrial: boolean;
  trialDaysLeft: number;
  isTrialEndingWithin24Hours: boolean;

  // Billing Information
  paymentStatus: OrganizationPaymentConfigReadPaymentStatus | undefined;
  subscriptionStatus: OrganizationPaymentConfigReadSubscriptionStatus | undefined;
  accountType: string | undefined;
  gracePeriodEndsAt: number | undefined;

  // Permissions
  canManageOrganizationBilling: boolean;
}

/**
 * Hook that returns comprehensive organization subscription status information.
 * Provides trial, payment, and subscription status throughout the subscription lifecycle.
 * Follows the same logic as useBillingStatusBanner but focuses on data retrieval.
 */
export const useOrganizationSubscriptionStatus = (options?: {
  refetchWithInterval?: boolean;
  onSuccessGetTrialStatusClb?: (isTrialEndedAndLockedOrDisabled: boolean) => void;
}): UseOrganizationSubscriptionStatusReturn => {
  const organizationInfo = useCurrentOrganizationInfo();
  const organizationId = organizationInfo?.organizationId;

  // Permission checks
  const canViewTrialStatus = useGeneratedIntent(Intent.ViewOrganizationTrialStatus, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });

  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};

  const isUnifiedTrialPlan = organizationInfo?.organizationPlanId === ORG_PLAN_IDS.UNIFIED_TRIAL;
  const isStandardTrialPlan = organizationInfo?.organizationPlanId === ORG_PLAN_IDS.STANDARD_TRIAL;
  const isStandardPlan = organizationInfo?.organizationPlanId === ORG_PLAN_IDS.STANDARD;

  // Conditional trial status fetching - only when payment status allows it, user has permissions, and organization's plan is a unified trial plan
  const shouldFetchTrialStatus =
    (billing?.paymentStatus === "uninitialized" ||
      billing?.paymentStatus === "okay" ||
      billing?.paymentStatus === "disabled" ||
      billing?.paymentStatus === "locked") &&
    canViewTrialStatus &&
    isUnifiedTrialPlan;

  const trialStatus = useOrganizationTrialStatus(organizationId, {
    enabled: shouldFetchTrialStatus,
    ...(options?.refetchWithInterval && {
      refetchInterval: options?.refetchWithInterval,
    }),
    ...(options?.onSuccessGetTrialStatusClb && {
      onSuccess: (data: OrganizationTrialStatusRead) => {
        const isTrialEndedAndLockedOrDisabled =
          data.trialStatus === "post_trial" &&
          (billing?.paymentStatus === "locked" || billing?.paymentStatus === "disabled");
        options?.onSuccessGetTrialStatusClb?.(isTrialEndedAndLockedOrDisabled);
      },
    }),
  });

  // Calculate remaining trial days
  const trialDaysLeft = useMemo(() => {
    if (!trialStatus?.trialEndsAt || trialStatus.trialStatus !== "in_trial") {
      return 0;
    }
    try {
      return Math.max(dayjs(trialStatus.trialEndsAt).diff(dayjs(), "days"), 0);
    } catch {
      return 0;
    }
  }, [trialStatus?.trialEndsAt, trialStatus?.trialStatus]);

  // Check if trial is ending within 24 hours
  const isTrialEndingWithin24Hours = useMemo(() => {
    if (!trialStatus?.trialEndsAt || trialStatus.trialStatus !== "in_trial") {
      return false;
    }
    try {
      const hoursLeft = dayjs(trialStatus.trialEndsAt).diff(dayjs(), "hours");
      return hoursLeft > 0 && hoursLeft <= 24;
    } catch {
      return false;
    }
  }, [trialStatus?.trialEndsAt, trialStatus?.trialStatus]);

  const isInTrial = trialStatus?.trialStatus === "in_trial";

  return {
    // Organization plan information
    isUnifiedTrialPlan,
    isStandardTrialPlan,
    isStandardPlan,

    // Trial Information
    trialStatus: trialStatus?.trialStatus,
    trialEndsAt: trialStatus?.trialEndsAt,
    isInTrial,
    trialDaysLeft,
    isTrialEndingWithin24Hours,

    // Billing Information
    paymentStatus: billing?.paymentStatus,
    subscriptionStatus: billing?.subscriptionStatus,
    accountType: billing?.accountType,
    gracePeriodEndsAt: billing?.gracePeriodEndsAt,

    // Permissions
    canManageOrganizationBilling,
  };
};
