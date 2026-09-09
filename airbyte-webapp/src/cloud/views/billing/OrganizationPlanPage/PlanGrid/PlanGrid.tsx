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

  const activeTier: PlanTier | null = !isSubscribed
    ? null
    : isStandardPlan
    ? "standard"
    : isPlusPlan
    ? "plus"
    : isProPlan || isSmePlan
    ? "pro"
    : isFlexPlan
    ? "flex"
    : null;
  const isTopTier = activeTier === "pro" || activeTier === "flex";

  const { data: subscription } = useGetOrganizationSubscriptionInfo(organizationId, activeTier !== null);
  const cancellationDate = subscription?.cancellationDate;

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
