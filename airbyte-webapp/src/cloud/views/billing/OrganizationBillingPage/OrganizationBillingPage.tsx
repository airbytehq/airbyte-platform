import React from "react";
import { FormattedMessage } from "react-intl";

import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";
import { PageContainer } from "components/ui/PageContainer";

import { SetupBillingAlertsLink } from "area/organization/components/SetupBillingAlertsLink";
import { isOrganizationSubscribed, useCurrentOrganizationId, useOrganizationPlan } from "area/organization/utils";
import { useHasInAppBillingAlerts } from "cloud/area/billing/utils/useHasInAppBillingAlerts";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useFormatCredits } from "core/utils/numberHelper";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { AccountBalance } from "./AccountBalance";
import { BillingAlerts } from "./BillingAlerts";
import { BillingBanners } from "./BillingBanners";
import { BillingInformation } from "./BillingInformation";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { SubscribeCards } from "./SubscribeCards";
import { Subscription } from "./Subscription";

export const OrganizationBillingPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_BILLING);

  const { formatCredits } = useFormatCredits();
  const { isProPlan, isFlexPlan } = useOrganizationPlan();

  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  const { data: subscriptionInfo } = useGetOrganizationSubscriptionInfo(
    organizationId,
    billing?.subscriptionStatus === "subscribed"
  );

  const showSubscribeCards = !isOrganizationSubscribed(billing);
  const hasInAppBillingAlerts = useHasInAppBillingAlerts();

  return (
    <PageContainer>
      <FlexContainer direction="column" gap="xl">
        <FlexContainer justifyContent="space-between" alignItems="center">
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.billing.title" />
          </Heading>
          {!showSubscribeCards && !hasInAppBillingAlerts && <SetupBillingAlertsLink />}
        </FlexContainer>

        {/* Show credits as a banner in case the user isn't on an active subscription and thus sees the susbcribe to airbyte cards. */}
        {showSubscribeCards && !!subscriptionInfo?.credits?.balance && subscriptionInfo?.credits?.balance > 0 && (
          <Box mb="none">
            <Message
              text={
                <FormattedMessage
                  id="settings.organization.billing.remainingCreditsBanner"
                  values={{
                    amount: formatCredits(subscriptionInfo.credits.balance),
                  }}
                />
              }
            />
          </Box>
        )}
        <BillingBanners />

        {showSubscribeCards ? (
          <SubscribeCards />
        ) : (
          <BorderedTiles>
            <Subscription />

            <AccountBalance />

            <BorderedTile>
              <BillingInformation />
            </BorderedTile>

            {!isProPlan && !isFlexPlan && (
              <BorderedTile>
                <PaymentMethod />
              </BorderedTile>
            )}
          </BorderedTiles>
        )}

        {hasInAppBillingAlerts && <BillingAlerts />}

        {billing?.subscriptionStatus !== "pre_subscription" && billing?.paymentStatus !== "uninitialized" && (
          <Box py="lg">
            <Invoices />
          </Box>
        )}
      </FlexContainer>
    </PageContainer>
  );
};
