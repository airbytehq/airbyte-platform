import React, { useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { ORG_PLAN_IDS } from "components/ui/BrandingBadge/BrandingBadge";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { PageContainer } from "components/ui/PageContainer";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetOrganizationSubscriptionInfo, useOrganization, useOrgInfo } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useModalService } from "core/services/Modal";
import { links } from "core/utils/links";
import { useFormatCredits } from "core/utils/numberHelper";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { AccountBalance } from "./AccountBalance";
import { BillingBanners } from "./BillingBanners";
import { BillingInformation } from "./BillingInformation";
import { CloudSubscriptionSuccessModal } from "./CloudSubscriptionSuccessModal";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { SubscribeCards } from "./SubscribeCards";
import { Subscription } from "./Subscription";

export const OrganizationBillingPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_BILLING);

  const [searchParams, setSearchParams] = useSearchParams();
  const { openModal } = useModalService();
  const { formatCredits } = useFormatCredits();
  const { isStandardPlan, isProPlan, isFlexPlan } = useOrganizationSubscriptionStatus();

  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { email } = useOrganization(organizationId);
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  const { data: subscriptionInfo } = useGetOrganizationSubscriptionInfo(
    organizationId,
    billing?.subscriptionStatus === "subscribed"
  );

  // We need to show the subscribe state if the user is either not on a subscription in Orb (anymore)
  // or if subscribed, but still in an uninitialized payment status, i.e. still in trial without a payment method.
  const showSubscribeCards =
    billing?.subscriptionStatus !== "subscribed" ||
    (billing?.subscriptionStatus === "subscribed" && billing.paymentStatus === "uninitialized");

  // Handle subscription success modal
  useEffect(() => {
    const cloudSubscriptionSetup = searchParams.get("cloudSubscriptionSetup");
    const previousPlan = searchParams.get("previousPlan");
    if (cloudSubscriptionSetup === "success" && isStandardPlan && previousPlan === ORG_PLAN_IDS.UNIFIED_TRIAL) {
      openModal({
        title: <FormattedMessage id="settings.organization.billing.subscriptionSuccess.title" />,
        content: CloudSubscriptionSuccessModal,
        size: "sm",
      });
      // Clean up URL parameter
      const newSearchParams = new URLSearchParams(searchParams);
      newSearchParams.delete("cloudSubscriptionSetup");
      setSearchParams(newSearchParams, { replace: true });
    }
  }, [searchParams, setSearchParams, openModal, isStandardPlan]);

  return (
    <PageContainer>
      <FlexContainer direction="column" gap="xl">
        <FlexContainer justifyContent="space-between" alignItems="center">
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.billing.title" />
          </Heading>
          {!showSubscribeCards && (
            <FlexItem>
              <Text size="sm">
                <ExternalLink
                  href={links.billingNotificationsForm
                    .replace("{organizationId}", organizationId)
                    .replace("{email}", email ?? "")}
                  opensInNewTab
                >
                  <FlexContainer alignItems="center" gap="xs">
                    <Icon type="bell" size="sm" />
                    <FormattedMessage id="settings.organization.billing.setupNotifications" />
                  </FlexContainer>
                </ExternalLink>
              </Text>
            </FlexItem>
          )}
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

        {billing?.subscriptionStatus !== "pre_subscription" && billing?.paymentStatus !== "uninitialized" && (
          <Box py="lg">
            <Invoices />
          </Box>
        )}
      </FlexContainer>
    </PageContainer>
  );
};
