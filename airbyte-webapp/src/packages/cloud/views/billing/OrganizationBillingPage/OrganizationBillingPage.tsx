import { FormattedMessage } from "react-intl";

import { PageContainer } from "components/PageContainer";
import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useGetOrganizationSubscriptionInfo } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { links } from "core/utils/links";
import { useFormatCredits } from "core/utils/numberHelper";

import { AccountBalance } from "./AccountBalance";
import { BillingBanners } from "./BillingBanners";
import { BillingInformation } from "./BillingInformation";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { SubscribeCards } from "./SubscribeCards";
import { Subscription } from "./Subscription";

export const OrganizationBillingPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_BILLING);

  const { formatCredits } = useFormatCredits();

  const { organizationId } = useCurrentWorkspace();
  const { billing } = useCurrentOrganizationInfo();
  const { data: subscriptionInfo } = useGetOrganizationSubscriptionInfo(
    organizationId,
    billing?.subscriptionStatus === "subscribed"
  );

  // We need to show the subscribe state if the user is either not on a subscription in Orb (anymore)
  // or if subscribed, but still in an uninitialized payment status, i.e. still in trial without a payment method.
  const showSubscribeCards =
    billing?.subscriptionStatus !== "subscribed" ||
    (billing?.subscriptionStatus === "subscribed" && billing.paymentStatus === "uninitialized");

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
                  href={links.billingNotificationsForm.replace("{organizationId}", organizationId)}
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

            <BorderedTile>
              <PaymentMethod />
            </BorderedTile>
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
