import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";
import { PageContainer } from "components/ui/PageContainer";

import { isOrganizationSubscribed, useCurrentOrganizationId } from "area/organization/utils";
import { useOrganizationPlan } from "area/organization/utils/useOrganizationPlan";
import {
  PricingComparisonLink,
  PlusPlanCard,
  ProPlanCard,
  StandardPlanCard,
} from "cloud/area/billing/components/PlanCards";
import { useLinkToBillingPage } from "cloud/area/billing/utils/useLinkToBillingPage";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ActivePlanCard, ActivePlanTier } from "./ActivePlanCard";

const OrganizationPlanPageContent: React.FC = () => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};

  const isSubscribed = isOrganizationSubscribed(billing);

  const { isStandardPlan, isPlusPlan, isProPlan, isSmePlan, isFlexPlan } = useOrganizationPlan();
  const isLockedSubscription = billing?.paymentStatus === "locked";
  const isTopTier = isProPlan || isSmePlan || isFlexPlan;

  const activeTier: ActivePlanTier | null = isStandardPlan
    ? "standard"
    : isPlusPlan
    ? "plus"
    : isTopTier
    ? "pro"
    : null;

  const showActivePlanSection = isSubscribed && activeTier !== null;

  const {
    data: subscription,
    isLoading: subscriptionLoading,
    isError: subscriptionError,
  } = useGetOrganizationSubscriptionInfo(organizationId, showActivePlanSection);

  return (
    <PageContainer>
      <FlexContainer direction="column" gap="xl">
        <Heading as="h1" size="md">
          <FormattedMessage id="settings.organization.billing.plan.title" />
        </Heading>

        {showActivePlanSection ? (
          <>
            <ActivePlanCard
              tier={activeTier}
              planName={subscription?.entitlementPlan?.planName ?? subscription?.name}
              isLoading={subscriptionLoading}
              isError={subscriptionError}
              cancellationDate={subscription?.cancellationDate}
            />
            {!isTopTier && (
              <FlexContainer direction="column" gap="lg">
                {isStandardPlan && <PlusPlanCard disabled={isLockedSubscription} isPaidPlan />}
                {isPlusPlan && <StandardPlanCard disabled={isLockedSubscription} mode="downgrade" />}
                <ProPlanCard />
                <PricingComparisonLink />
              </FlexContainer>
            )}
          </>
        ) : (
          <>
            <Message
              type="info"
              text={formatMessage({ id: "settings.organization.billing.plan.noActivePlan.title" })}
              secondaryText={<FormattedMessage id="settings.organization.billing.plan.noActivePlan.description" />}
            />
            <FlexContainer direction="column" gap="lg">
              <StandardPlanCard disabled={isLockedSubscription} mode="subscribe" />
              <PlusPlanCard disabled={isLockedSubscription} isPaidPlan={false} />
              <ProPlanCard />
              <PricingComparisonLink />
            </FlexContainer>
          </>
        )}
      </FlexContainer>
    </PageContainer>
  );
};

export const OrganizationPlanPage: React.FC = () => {
  const billingPagePath = useLinkToBillingPage();
  const isSelfServePlusPlanEnabled = useExperiment("billing.selfServePlusPlan");

  if (!isSelfServePlusPlanEnabled) {
    return <Navigate to={billingPagePath} replace />;
  }

  return <OrganizationPlanPageContent />;
};
