import dayjs from "dayjs";
import { useMemo } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useOrganizationTrialStatus, useOrgInfo } from "core/api";
import {
  OrganizationTrialStatusReadTrialStatus,
  OrganizationPaymentConfigReadPaymentStatus,
  OrganizationPaymentConfigReadSubscriptionStatus,
  ISO8601DateTime,
  OrganizationTrialStatusRead,
} from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export interface UseOrganizationSubscriptionStatusReturn {
  // Trial Information
  isInTrial: boolean;
  trialDaysLeft: number;
  trialStatus: OrganizationTrialStatusReadTrialStatus | undefined;
  trialEndsAt: ISO8601DateTime | undefined;
  isTrialEndingWithin24Hours: boolean;

  // Payment/Subscription Information
  paymentStatus: OrganizationPaymentConfigReadPaymentStatus | undefined;
  subscriptionStatus: OrganizationPaymentConfigReadSubscriptionStatus | undefined;

  // Computed States
  isTrialWithPaymentMethod: boolean;
  isTrialWithoutPaymentMethod: boolean;
  isPostTrialUnsubscribed: boolean;

  // Permissions
  canViewTrialStatus: boolean;
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
  const organizationId = useCurrentOrganizationId();

  // Permission checks
  const canViewTrialStatus = useGeneratedIntent(Intent.ViewOrganizationTrialStatus, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });

  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};

  // Conditional trial status fetching - only when payment status allows it and user has permissions
  const shouldFetchTrialStatus =
    (billing?.paymentStatus === "uninitialized" ||
      billing?.paymentStatus === "okay" ||
      billing?.paymentStatus === "disabled" ||
      billing?.paymentStatus === "locked") &&
    canViewTrialStatus;

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

  // Computed states
  const isInTrial = trialStatus?.trialStatus === "in_trial";

  const isTrialWithPaymentMethod = useMemo(() => {
    return trialStatus?.trialStatus === "in_trial" && billing?.paymentStatus === "okay";
  }, [trialStatus?.trialStatus, billing?.paymentStatus]);

  const isTrialWithoutPaymentMethod = useMemo(() => {
    return trialStatus?.trialStatus === "in_trial" && billing?.paymentStatus === "uninitialized";
  }, [trialStatus?.trialStatus, billing?.paymentStatus]);

  const isPostTrialUnsubscribed = useMemo(() => {
    return trialStatus?.trialStatus === "post_trial" && billing?.subscriptionStatus !== "subscribed";
  }, [trialStatus?.trialStatus, billing?.subscriptionStatus]);

  return {
    // Trial Information
    isInTrial,
    trialDaysLeft,
    trialStatus: trialStatus?.trialStatus,
    trialEndsAt: trialStatus?.trialEndsAt,
    isTrialEndingWithin24Hours,

    // Payment/Subscription Information
    paymentStatus: billing?.paymentStatus,
    subscriptionStatus: billing?.subscriptionStatus,

    // Computed States
    isTrialWithPaymentMethod,
    isTrialWithoutPaymentMethod,
    isPostTrialUnsubscribed,

    // Permissions
    canViewTrialStatus,
    canManageOrganizationBilling,
  };
};
