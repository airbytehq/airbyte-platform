import { FormattedMessage, useIntl } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { PageContainer } from "components/PageContainer";
import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useGetOrganizationBillingBalance } from "core/api";
import { links } from "core/utils/links";
import { useFormatCredits } from "core/utils/numberHelper";

import { AccountBalance } from "./AccountBalance";
import { BillingBanners } from "./BillingBanners";
import { BillingInformation } from "./BillingInformation";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { useRedirectToCustomerPortal } from "../useRedirectToCustomerPortal";

export const OrganizationBillingPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { organizationId } = useCurrentWorkspace();
  const { billing } = useCurrentOrganizationInfo();
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("portal");
  const { formatCredits } = useFormatCredits();

  const { data: balance } = useGetOrganizationBillingBalance(organizationId);

  return (
    <PageContainer>
      {billing && billing.paymentStatus !== "uninitialized" ? (
        <FlexContainer direction="column" gap="xl">
          <FlexContainer justifyContent="space-between" alignItems="center">
            <Heading as="h1" size="md">
              <FormattedMessage id="settings.organization.billing.title" />
            </Heading>
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
          </FlexContainer>

          <BillingBanners />

          <BorderedTiles>
            <AccountBalance />

            <BorderedTile>
              <BillingInformation />
            </BorderedTile>

            <BorderedTile>
              <PaymentMethod />
            </BorderedTile>
          </BorderedTiles>

          <Box py="lg">
            <Invoices />
          </Box>
        </FlexContainer>
      ) : (
        <FlexContainer gap="md" direction="column">
          {!!balance?.credits?.balance && balance?.credits?.balance > 0 && (
            <Message
              text={
                <FormattedMessage
                  id="settings.organization.billing.remainingCreditsBanner"
                  values={{
                    amount: formatCredits(balance.credits.balance),
                  }}
                />
              }
            />
          )}
          <BillingBanners />
          <Box py="2xl">
            <EmptyState
              text={
                <FlexContainer direction="column" alignItems="center" gap="lg">
                  <FlexItem>{formatMessage({ id: "settings.organization.billing.notSetUp" })}</FlexItem>
                  <FlexItem>
                    <FormattedMessage
                      id="settings.organization.billing.notSetUpDetails"
                      values={{
                        lnk: (node: React.ReactNode) => (
                          <ExternalLink href={links.creditDescription} opensInNewTab>
                            {node}
                          </ExternalLink>
                        ),
                      }}
                    />
                  </FlexItem>
                </FlexContainer>
              }
              button={
                <Button variant="primary" onClick={goToCustomerPortal} isLoading={redirecting}>
                  <FormattedMessage id="settings.organization.billing.paymentMethod.add" />
                </Button>
              }
            />
          </Box>
        </FlexContainer>
      )}
    </PageContainer>
  );
};
