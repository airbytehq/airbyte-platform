import React from "react";

import { isOrganizationSubscribed, useCurrentOrganizationId } from "area/organization/utils";
import { useOrganizationPlan } from "area/organization/utils/useOrganizationPlan";
import { PricingComparisonLink } from "cloud/area/billing/components/PlanCards";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { FlexPlanGridCard } from "./FlexPlanGridCard";
import styles from "./PlanGrid.module.scss";
import { PlusPlanGridCard } from "./PlusPlanGridCard";
import { ProPlanGridCard } from "./ProPlanGridCard";
import { StandardPlanGridCard } from "./StandardPlanGridCard";

type PlanTier = "standard" | "plus" | "pro" | "flex";

export const PlanGrid: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};

  const isSubscribed = isOrganizationSubscribed(billing);
  const isLockedSubscription = billing?.paymentStatus === "locked";

  const { isStandardPlan, isPlusPlan, isProPlan, isSmePlan, isFlexPlan } = useOrganizationPlan();

  const { data: subscription } = useGetOrganizationSubscriptionInfo(organizationId, isSubscribed);
  const cancellationDate = subscription?.cancellationDate;
  const selfServePlan = subscription?.selfServePlan;

  // The billing subscription decides which self-serve plan is current. The entitlement plan is the
  // fallback for plans that are not self-serve (Pro, SME, Flex) and while the subscription is loading.
  const selfServeTier: PlanTier | null =
    selfServePlan === undefined ? null : selfServePlan === "standard" ? "standard" : "plus";
  const entitlementTier: PlanTier | null = isStandardPlan
    ? "standard"
    : isPlusPlan
    ? "plus"
    : isProPlan || isSmePlan
    ? "pro"
    : isFlexPlan
    ? "flex"
    : null;
  const activeTier: PlanTier | null = isSubscribed ? selfServeTier ?? entitlementTier : null;
  const isTopTier = activeTier === "pro" || activeTier === "flex";

  return (
    <>
      <div className={styles.page}>
        <div className={styles.grid}>
          <StandardPlanGridCard
            disabled={isLockedSubscription || isTopTier}
            mode={activeTier === "plus" ? "downgrade" : "subscribe"}
            isCurrentPlan={activeTier === "standard"}
            cancellationDate={cancellationDate}
          />
          <PlusPlanGridCard
            disabled={isLockedSubscription || isTopTier}
            isPaidPlan={activeTier === "standard"}
            isCurrentPlan={activeTier === "plus"}
            currentPlan={selfServePlan}
            cancellationDate={cancellationDate}
          />
          <ProPlanGridCard
            disabled={activeTier === "flex"}
            isCurrentPlan={activeTier === "pro"}
            cancellationDate={cancellationDate}
          />
          <FlexPlanGridCard isCurrentPlan={activeTier === "flex"} cancellationDate={cancellationDate} />
        </div>
      </div>
      <PricingComparisonLink />
    </>
  );
};
